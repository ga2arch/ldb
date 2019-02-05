(ns db
  (:import (java.nio.file Files Path)
           (org.lmdbjava Env DbiFlags Dbi PutFlags KeyRange Txn)
           (java.io File)
           (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.util Iterator)))

(defprotocol ADatom
  (to-eavt [this])
  (to-aevt [this]))

(defrecord Datom [eid attr value tid op])

(defprotocol ALdb
  (get-index [this name])
  (update-index [this name key datoms])
  (from-index [this name key])
  (append-log [this datoms]))

(defprotocol AConnection
  (txn-read [this])
  (txn-write [this])
  (txn-commit [this txn])
  (with-txn [this type fn])

  (get-key [this key] [this txn key])
  (put-key [this key vals] [this txn key vals])
  (del-key [this key] [this txn key])
  (iterate-key [this key fn] [this txn key fn])

  (close [this]))

(defprotocol AStorage
  (open-conn [this options]))

(defprotocol ADatabase
  (conn [this]))

(defrecord Database [name file storage]
  ADatabase
  (conn [this]
    (open-conn storage this)))

(def bkey (ByteBuffer/allocateDirect 511))
(def bval (ByteBuffer/allocateDirect 2000))

(defn key->bytes
  [data]
  (-> bkey
      (.put (.getBytes data "UTF-8"))
      (.flip))
  bkey)

(defn val->bytes
  [data]
  (-> bval
      (.put (.getBytes data "UTF-8"))
      (.flip))
  bval)

(defn clear
  [& xs]
  (doseq [[^ByteBuffer x] xs]
    (.clear x)))

(defn decode
  [buff]
  (str (.decode StandardCharsets/UTF_8 buff)))

(deftype LmdbConnection [^Env env ^Dbi db]
  AConnection
  (txn-read [this]
    (.txnRead env))

  (txn-write [this]
    (.txnWrite env))

  (txn-commit [this txn]
    (.commit txn))

  (with-txn [this type fun]
    (with-open [txn (case type
                      :read
                      (txn-read this)

                      :write
                      (txn-write this))]
      (let [res (fun txn)]
        (when (= type :write)
          (txn-commit this txn))
        res)))

  (put-key [this key vals]
    (with-txn this :write (fn [txn] (put-key this txn key vals))))

  (put-key [this txn key vals]
    (key->bytes key)
    (doseq [val vals]
      (.put db txn bkey (val->bytes val) (into-array PutFlags []))
      (clear [bval]))
    (clear [bkey bval]))

  (get-key [this key]
    (with-txn this :read (fn [txn] (get-key this txn key))))

  (get-key [this txn key]
    (key->bytes key)
    (with-open [^Iterator it (.iterate db txn (KeyRange/closed bkey bkey))]
      (let [vals (volatile! [])]
        (while (.hasNext it)
          (vswap! vals (fn [vals] (conj vals (decode (.val (.next it)))))))
        (clear [bkey])
        @vals)))

  (iterate-key [this txn key fun]
    (key->bytes key)
    (with-open [^Iterator it (.iterate db txn (KeyRange/closed bkey bkey))]
      (while (.hasNext it)
        (let [val (decode (.val (.next it)))]
          (fun it key val))))
    (clear [bkey]))

  (iterate-key [this key fun]
    (with-txn this :write (fn [txn] (iterate-key this txn key fun))))

  (del-key [this txn key]
    (.delete db txn (key->bytes key))
    (clear [bkey]))

  (del-key [this key]
    (with-txn this :write (fn [txn] (del-key this txn key))))

  (close [this]
    (.close env)))

(deftype Lmdb []
  AStorage
  (open-conn [this db]
    (let [env (-> (Env/create)
                  (.setMapSize 10485760)
                  (.setMaxDbs 5)
                  (.open (.-file db) nil))
          db (.openDbi env
                       (.-name db)
                       (into-array DbiFlags [DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT]))]
      (->LmdbConnection env db))))


(def db (->Database "prova" (File. ".") (->Lmdb)))
(def mconn (conn db))