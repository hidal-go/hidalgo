package mongo

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/hidal-go/hidalgo/base"
	"github.com/hidal-go/hidalgo/legacy/nosql"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

const Name = "mongo"

var _ nosql.BatchInserter = (*DB)(nil)

func Traits() nosql.Traits {
	return nosql.Traits{
		TimeInMs: true,
	}
}

func init() {
	nosql.Register(nosql.Registration{
		Registration: base.Registration{
			Name: Name, Title: "MongoDB",
			Local: false, Volatile: false,
		},
		Traits: Traits(),
		Open: func(addr, ns string, opt nosql.Options) (nosql.Database, error) {
			db, err := Dial(addr, ns, opt)
			if err != nil {
				return nil, err
			}
			return db, nil
		},
	})
}

func dialMongo(addr, dbName string, noSqloptions nosql.Options) (*mongo.Client, error) {
	if connVal, ok := noSqloptions["session"]; ok {
		if conn, ok := connVal.(*mongo.Client); ok {
			return conn, nil
		}
	}
	if strings.HasPrefix(addr, "mongodb://") || strings.ContainsAny(addr, `@/\`) {
		// full mongodb url
		client, err := mongo.NewClient(options.Client().ApplyURI(addr))
		if err != nil {
			return nil, err
		}
		err = client.Connect(context.TODO())
		if err != nil {
			return nil, err
		}

		return client, err
	}
	connString := "mongodb://"

	if user := noSqloptions.GetString("username", ""); user != "" {
		connString = fmt.Sprintf("%s%s:%s", connString, url.QueryEscape(user), url.QueryEscape(noSqloptions.GetString("password", "")))
	}
	connString = fmt.Sprintf("%s@%s/%s", connString, addr, dbName)
	client, err := mongo.NewClient(options.Client().ApplyURI(connString))
	if err != nil {
		return nil, err
	}
	return client, client.Connect(context.TODO())
}

func New(sess *mongo.Client, dbName string) (*DB, error) {
	return &DB{
		sess: sess, db: sess.Database(dbName),
		colls: make(map[string]collection),
	}, nil
}

func Dial(addr, dbName string, opt nosql.Options) (*DB, error) {
	// the dbName parameter is actually the defaultDatabase name, check if we had an override via the options
	dbOverride := opt.GetString("database_name", dbName)

	sess, err := dialMongo(addr, dbOverride, opt)
	if err != nil {
		return nil, err
	}
	return New(sess, dbOverride)
}

type collection struct {
	c         *mongo.Collection
	secondary []nosql.Index
	primary   nosql.Index
	compPK    bool // compose PK from existing keys; if false, use _id instead of target field
}

type DB struct {
	sess  *mongo.Client
	db    *mongo.Database
	colls map[string]collection
}

func (db *DB) Close() error {
	db.sess.Disconnect(context.TODO())
	return nil
}

func (db *DB) EnsureIndex(ctx context.Context, col string, primary nosql.Index, secondary []nosql.Index) error {
	if primary.Type != nosql.StringExact {
		return fmt.Errorf("unsupported type of primary index: %v", primary.Type)
	}
	c := db.db.Collection(col)
	compPK := len(primary.Fields) != 1
	if compPK {
		indexView := c.Indexes()
		indexOptions := options.Index().SetUnique(true)
		keys := make(bson.D, 0, len(primary.Fields))

		for _, field := range primary.Fields {
			keys = append(keys, primitive.E{Key: field, Value: bsonx.Int32(1)})
		}
		index := mongo.IndexModel{
			Keys:    keys,
			Options: indexOptions,
		}

		_, err := indexView.CreateOne(ctx, index)
		if err != nil {
			return err
		}
	}
	for _, ind := range secondary {
		indexView := c.Indexes()
		indexOptions := options.Index().SetUnique(false).SetSparse(true).SetBackground(true)
		keys := make(bson.D, 0, len(ind.Fields))

		for _, field := range ind.Fields {
			keys = append(keys, primitive.E{Key: field, Value: bsonx.Int32(1)})
		}
		index := mongo.IndexModel{
			Keys:    keys,
			Options: indexOptions,
		}

		_, err := indexView.CreateOne(ctx, index)
		if err != nil {
			return err
		}
	}
	db.colls[col] = collection{
		c:         c,
		compPK:    compPK,
		primary:   primary,
		secondary: secondary,
	}
	return nil
}

func toBsonValue(v nosql.Value) interface{} {
	switch v := v.(type) {
	case nil:
		return nil
	case nosql.Document:
		return toBsonDoc(v)
	case nosql.Strings:
		return []string(v)
	case nosql.String:
		return string(v)
	case nosql.Int:
		return int64(v)
	case nosql.Float:
		return float64(v)
	case nosql.Bool:
		return bool(v)
	case nosql.Time:
		return time.Time(v)
	case nosql.Bytes:
		return []byte(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func fromBsonValue(v interface{}) nosql.Value {
	switch v := v.(type) {
	case nil:
		return nil
	case []interface{}:
		arr := make(nosql.Strings, 0, len(v))
		for _, s := range v {
			sv := fromBsonValue(s)
			str, ok := sv.(nosql.String)
			if !ok {
				panic(fmt.Errorf("unsupported value in array: %T", sv))
			}
			arr = append(arr, string(str))
		}
		return arr
	case primitive.ObjectID:
		return nosql.String(objidString(v))
	case primitive.M:
		return fromBsonDoc(v)
	case primitive.A:
		arr := make(nosql.Strings, 0, len(v))
		for _, s := range v {
			sv := fromBsonValue(s)
			str, ok := sv.(nosql.String)
			if !ok {
				panic(fmt.Errorf("unsupported value in array: %T", sv))
			}
			arr = append(arr, string(str))
		}
		return arr
	case string:
		return nosql.String(v)
	case int:
		return nosql.Int(v)
	case int64:
		return nosql.Int(v)
	case int32:
		return nosql.Int(v)
	case float64:
		return nosql.Float(v)
	case bool:
		return nosql.Bool(v)
	case time.Time:
		return nosql.Time(v)
	case primitive.DateTime:
		const ms = int64(time.Millisecond)
		return nosql.Time(time.Unix(int64(v)/1000, int64(v)%1000*ms))
	case []byte:
		return nosql.Bytes(v)
	default:
		panic(fmt.Errorf("unsupported type: %T", v))
	}
}

func toBsonDoc(d nosql.Document) primitive.M {
	if d == nil {
		return nil
	}
	m := make(primitive.M, len(d))
	for k, v := range d {
		m[k] = toBsonValue(v)
	}
	return m
}

func fromBsonDoc(d primitive.M) nosql.Document {
	if d == nil {
		return nil
	}
	m := make(nosql.Document, len(d))
	for k, v := range d {
		m[k] = fromBsonValue(v)
	}
	return m
}

const idField = "_id"

func (c *collection) getKey(m primitive.M) nosql.Key {
	if !c.compPK {
		// key field renamed to _id - just return it
		if v, ok := m[idField].(string); ok {
			return nosql.Key{v}
		}
		return nil
	}
	// key field computed from multiple source fields
	// get source fields from document in correct order
	key := make(nosql.Key, 0, len(c.primary.Fields))
	for _, f := range c.primary.Fields {
		s, _ := m[f].(string)
		key = append(key, s)
	}
	return key
}

func (c *collection) setKey(m primitive.M, key nosql.Key) {
	if !c.compPK {
		// delete source field, since we already added it as _id
		delete(m, c.primary.Fields[0])
	} else {
		for i, f := range c.primary.Fields {
			m[f] = string(key[i])
		}
	}
}

func (c *collection) convDoc(m primitive.M) nosql.Document {
	if c.compPK {
		// key field computed from multiple source fields - remove it
		delete(m, idField)
	} else {
		// key field renamed - set correct name
		if v, ok := m[idField].(string); ok {
			delete(m, idField)
			m[c.primary.Fields[0]] = string(v)
		}
	}
	return fromBsonDoc(m)
}

func getOrGenID(key nosql.Key) (nosql.Key, string) {
	var mid string
	if key == nil {
		// TODO: maybe allow to pass custom key types as nosql.Key
		oid := objidString(primitive.NewObjectID())
		mid = oid
		key = nosql.Key{oid}
	} else {
		mid = compKey(key)
	}
	return key, mid
}

func (c *collection) convIns(key nosql.Key, d nosql.Document) (nosql.Key, primitive.M) {
	m := toBsonDoc(d)

	var mid string
	key, mid = getOrGenID(key)
	m[idField] = mid
	c.setKey(m, key)

	return key, m
}

func objidString(id primitive.ObjectID) string {
	return base64.StdEncoding.EncodeToString(id[:])
}

func compKey(key nosql.Key) string {
	if len(key) == 1 {
		return key[0]
	}
	return strings.Join(key, "")
}

func (db *DB) Insert(ctx context.Context, col string, key nosql.Key, d nosql.Document) (nosql.Key, error) {
	c, ok := db.colls[col]
	if !ok {
		return nil, fmt.Errorf("collection %q not found", col)
	}
	key, m := c.convIns(key, d)
	if _, err := c.c.InsertOne(ctx, m); err != nil {
		return nil, err
	}
	return key, nil
}

func (db *DB) FindByKey(ctx context.Context, col string, key nosql.Key) (nosql.Document, error) {
	c := db.colls[col]

	res := c.c.FindOne(ctx, primitive.M{"_id": compKey(key)})
	var m primitive.M

	if err := res.Decode(&m); err == mongo.ErrNoDocuments {
		return nil, nosql.ErrNotFound
	} else if err != nil {
		return nil, err
	}
	return c.convDoc(m), nil
}

func (db *DB) Query(col string) nosql.Query {
	c := db.colls[col]
	return &Query{c: &c}
}

func (db *DB) Update(col string, key nosql.Key) nosql.Update {
	c := db.colls[col]
	return &Update{col: &c, key: key, update: make(primitive.M)}
}

func (db *DB) Delete(col string) nosql.Delete {
	c := db.colls[col]
	return &Delete{col: &c}
}

func buildFilters(filters []nosql.FieldFilter) primitive.M {
	m := make(primitive.M, len(filters))
	for _, f := range filters {
		name := strings.Join(f.Path, ".")
		v := toBsonValue(f.Value)
		if f.Filter == nosql.Equal {
			m[name] = v
			continue
		}
		var mf primitive.M
		switch mp := m[name].(type) {
		case nil:
		case primitive.M:
			mf = mp
		default:
			continue
		}
		if mf == nil {
			mf = make(primitive.M)
		}
		switch f.Filter {
		case nosql.NotEqual:
			mf["$ne"] = v
		case nosql.GT:
			mf["$gt"] = v
		case nosql.GTE:
			mf["$gte"] = v
		case nosql.LT:
			mf["$lt"] = v
		case nosql.LTE:
			mf["$lte"] = v
		case nosql.Regexp:
			pattern, ok := f.Value.(nosql.String)
			if !ok {
				panic(fmt.Errorf("unsupported regexp argument: %v", f.Value))
			}
			mf["$regex"] = pattern
		default:
			panic(fmt.Errorf("unsupported filter: %v", f.Filter))
		}
		m[name] = mf
	}
	return m
}

func mergeFilters(dst, src primitive.M) {
	for k, v := range src {
		dst[k] = v
	}
}

type Query struct {
	c     *collection
	query primitive.M
	limit int
}

func (q *Query) WithFields(filters ...nosql.FieldFilter) nosql.Query {
	m := buildFilters(filters)
	if q.query == nil {
		q.query = m
	} else {
		mergeFilters(q.query, m)
	}
	return q
}

func (q *Query) Limit(n int) nosql.Query {
	q.limit = n
	return q
}

func (q *Query) build() (*mongo.Cursor, error) {
	var m interface{} = bson.D{}
	if q.query != nil {
		m = q.query
	}
	findOptions := options.Find()
	if q.limit > 0 {
		findOptions.SetLimit(int64(q.limit))
	}

	qu, err := q.c.c.Find(context.TODO(), m, findOptions)

	return qu, err
}

func (q *Query) Count(ctx context.Context) (int64, error) {
	var m interface{} = bson.D{}
	if q.query != nil {
		m = q.query
	}
	countOptions := options.Count()

	if q.limit > 0 {
		countOptions.SetLimit(int64(q.limit))
	}

	count, err := q.c.c.CountDocuments(ctx, m, countOptions)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (q *Query) One(ctx context.Context) (nosql.Document, error) {
	m := &primitive.M{}
	cursor, err := q.build()
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return nil, nosql.ErrNotFound
	}
	if err := cursor.Decode(m); err != nil {
		return nil, err
	}
	return q.c.convDoc(*m), nil
}

func (q *Query) Iterate() nosql.DocIterator {
	it, err := q.build()
	if err != nil {
		return &Iterator{it: it, err: err, c: q.c}
	}
	return &Iterator{it: it, c: q.c}
}

type Iterator struct {
	c   *collection
	err error
	it  *mongo.Cursor
	res primitive.M
}

func (it *Iterator) Next(ctx context.Context) bool {
	elem := make(primitive.M)

	if it.it == nil {
		return false
	}
	if !it.it.Next(ctx) {
		return false
	}

	if err := it.it.Decode(&elem); err == nil {
		it.res = elem
	}

	return true
}

func (it *Iterator) Err() error {
	if it.err != nil {
		return it.err
	}
	if it.it != nil {
		return it.it.Err()
	}
	return nil
}

func (it *Iterator) Close() error {
	if it.it != nil {
		return it.it.Close(context.TODO())
	}
	return nil
}

func (it *Iterator) Key() nosql.Key {
	return it.c.getKey(it.res)
}

func (it *Iterator) Doc() nosql.Document {
	return it.c.convDoc(it.res)
}

type Delete struct {
	col   *collection
	query primitive.M
}

func (d *Delete) WithFields(filters ...nosql.FieldFilter) nosql.Delete {
	m := buildFilters(filters)
	if d.query == nil {
		d.query = m
	} else {
		mergeFilters(d.query, m)
	}
	return d
}

func (d *Delete) Keys(keys ...nosql.Key) nosql.Delete {
	if len(keys) == 0 {
		return d
	}
	m := make(primitive.M, 1)
	if len(keys) == 1 {
		m[idField] = compKey(keys[0])
	} else {
		ids := make([]string, 0, len(keys))
		for _, k := range keys {
			ids = append(ids, compKey(k))
		}
		m[idField] = primitive.M{"$in": ids}
	}
	if d.query == nil {
		d.query = m
	} else {
		mergeFilters(d.query, m)
	}
	return d
}

func (d *Delete) Do(ctx context.Context) error {
	var qu interface{}
	if d.query != nil {
		qu = d.query
	} else {
		qu = primitive.M{}
	}
	_, err := d.col.c.DeleteMany(ctx, qu)

	return err
}

type Update struct {
	col    *collection
	upsert primitive.M
	update primitive.M
	key    nosql.Key
}

func (u *Update) Inc(field string, dn int) nosql.Update {
	inc, _ := u.update["$inc"].(primitive.M)
	if inc == nil {
		inc = make(primitive.M)
	}
	inc[field] = dn
	u.update["$inc"] = inc
	return u
}

func (u *Update) Push(field string, v nosql.Value) nosql.Update {
	push, _ := u.update["$push"].(primitive.M)
	if push == nil {
		push = make(primitive.M)
	}
	push[field] = toBsonValue(v)
	u.update["$push"] = push
	return u
}

func (u *Update) Upsert(d nosql.Document) nosql.Update {
	u.upsert = toBsonDoc(d)
	if u.upsert == nil {
		u.upsert = make(primitive.M)
	}
	u.col.setKey(u.upsert, u.key)
	return u
}

func (u *Update) Do(ctx context.Context) error {
	idFilter := primitive.M{idField: compKey(u.key)}

	updateOptions := options.Update()
	if u.upsert != nil && len(u.upsert) != 0 {
		updateOptions.SetUpsert(true)
		u.update["$setOnInsert"] = u.upsert
	}
	_, err := u.col.c.UpdateOne(ctx, idFilter, u.update, updateOptions)

	return err
}

func (db *DB) BatchInsert(col string) nosql.DocWriter {
	c := db.colls[col]
	return &inserter{col: &c}
}

const batchSize = 100

type inserter struct {
	err   error
	col   *collection
	buf   []interface{}
	ikeys []nosql.Key
	keys  []nosql.Key
}

func (w *inserter) WriteDoc(ctx context.Context, key nosql.Key, d nosql.Document) error {
	if len(w.buf) >= batchSize {
		if err := w.Flush(ctx); err != nil {
			return err
		}
	}
	key, m := w.col.convIns(key, d)
	w.buf = append(w.buf, m)
	w.ikeys = append(w.ikeys, key)
	return nil
}

func (w *inserter) Flush(ctx context.Context) error {
	if len(w.buf) == 0 {
		return w.err
	}
	if _, err := w.col.c.InsertMany(ctx, w.buf, options.InsertMany()); err != nil {
		w.err = err
		return err
	}
	w.keys = append(w.keys, w.ikeys...)
	w.ikeys = w.ikeys[:0]
	w.buf = w.buf[:0]
	return w.err
}

func (w *inserter) Keys() []nosql.Key {
	return w.keys
}

func (w *inserter) Close() error {
	w.ikeys = nil
	w.buf = nil
	return w.err
}
