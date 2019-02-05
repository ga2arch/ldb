(ns db
  (:require [clojure.data.fressian :as fress])
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
  (with-txn [this type fun])

  (get-key [this key] [this txn key])
  (put-key [this key vals] [this txn key vals])
  (del-key [this key] [this txn key])
  (iterate-key [this key fun] [this txn key fun])

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

(defn encode-key
  [data]
  (doto bkey
    (.clear)
    (.put (.getBytes data StandardCharsets/UTF_8))
    (.flip)))

(defn encode-val
  [data]
  (doto bval
    (.clear)
    (.put (fress/write data))
    (.flip)))

(defn decode
  [data]
  (fress/read data))

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
    (doseq [val vals]
      (.put db txn (encode-key key) (encode-val val) (into-array PutFlags []))))

  (get-key [this key]
    (with-txn this :read (fn [txn] (get-key this txn key))))

  (get-key [this txn key]
    (let [ekey (encode-key key)]
      (with-open [^Iterator it (.iterate db txn (KeyRange/closed ekey ekey))]
        (let [vals (volatile! [])]
          (while (.hasNext it)
            (vswap! vals (fn [vals] (conj vals (decode (.val (.next it)))))))
          @vals))))

  (iterate-key [this txn key fun]
    (let [ekey (encode-key key)]
      (with-open [^Iterator it (.iterate db txn (KeyRange/closed ekey ekey))]
        (while (.hasNext it)
          (let [val (decode (.val (.next it)))]
            (fun it key val))))))

  (iterate-key [this key fun]
    (with-txn this :write (fn [txn] (iterate-key this txn key fun))))

  (del-key [this txn key]
    (.delete db txn (encode-key key)))

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