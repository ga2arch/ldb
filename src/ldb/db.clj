(ns ldb.db
  (:require [clojure.data.fressian :as fress])
  (:import (org.lmdbjava Env DbiFlags PutFlags KeyRange CursorIterator Dbi Txn)
           (java.io File)
           (java.nio ByteBuffer)
           (java.util UUID)
           (clojure.lang IReduceInit Named)))

(defrecord Connection [^Dbi eavt-current ^Dbi eavt-history
                       ^Dbi aevt-current ^Dbi aevt-history
                       ^Dbi avet-current ^Dbi avet-history
                       ^Dbi vaet-current ^Dbi vaet-history
                       ^Env env ^Dbi status ^Dbi ident])

(def bkey (ByteBuffer/allocateDirect 511))
(def bval (ByteBuffer/allocateDirect 2000))

(defn encode-key
  [data]
  (.clear bkey)
  (cond
    (int? data)
    (.putInt bkey data)
    :else
    (.put bkey (fress/write data)))
  (.flip bkey))

(defn encode-val
  [data]
  (doto bval
    (.clear)
    (.put (fress/write data))
    (.flip)))

(defn decode
  [data]
  (fress/read data))

(defn txn-read
  [^Connection conn]
  (.txnRead ^Env (.-env conn)))

(defn txn-write
  [^Connection conn]
  (.txnWrite ^Env (.-env conn)))

(defn txn-commit
  [^Txn txn]
  (.commit txn))

(defn put-key
  [^Dbi db ^Txn txn key val]
  (.put db txn (encode-key key) (encode-val val) (into-array PutFlags [])))

(defn scan-all
  ([^Dbi db ^Txn txn]
   (scan-all db txn false))
  ([^Dbi db ^Txn txn int-key?]
   (let [^CursorIterator it (.iterate db txn (KeyRange/all))]
     (let [vals (volatile! {})]
       (while (.hasNext it)
         (let [kv (.next it)]
           (vswap! vals (fn [vals] (update vals
                                           (if int-key?
                                             (.getInt (.key kv))
                                             (decode (.key kv)))
                                           (fn [a] ((fnil conj []) a (decode (.val kv)))))))))
       (into {} @vals)))))

(defn get-key
  [^Dbi db ^Txn txn key]
  (when-let [v (.get db txn (encode-key key))]
    (decode v)))

(defn scan-key
  [^Dbi db ^Txn txn key]
  (reify IReduceInit
    (reduce [this f init]
      (let [ekey (encode-key key)
            ^CursorIterator it (.iterate db txn (KeyRange/closed ekey ekey))]
        (try
          (loop [state init]
            (if (reduced? state)
              @state
              (if (.hasNext it)
                (recur (f state (decode (.val (.next it)))))
                state)))
          (finally
            (.close it)))))))

(defn del-key
  [^Dbi db ^Txn txn key]
  (.delete db txn (encode-key key)))

(defn del-kv
  [^Dbi db ^Txn txn key val]
  (.delete db txn (encode-key key) (encode-val val)))

(defn close
  [^Connection conn]
  (.close ^Env (.-env conn)))

(defn connect
  [^String filepath]
  (let [^Env env (-> (Env/create)
                     (.setMapSize 1099511627776)
                     (.setMaxDbs 7)
                     (.open (File. filepath) nil))]
    (letfn [(open-db [name & flags]
              (.openDbi env name (into-array DbiFlags flags)))]
      (map->Connection {:env          env
                        :eavt-current (open-db "eavt-current" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                        :eavt-history (open-db "eavt-history" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                        :aevt-current (open-db "aevt-current" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                        :aevt-history (open-db "aevt-history" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                        :status       (open-db "status" DbiFlags/MDB_CREATE)
                        :ident        (open-db "ident" DbiFlags/MDB_CREATE)}))))
;;

(defn entity-by-id*
  ([^Connection conn eid]
   (with-open [txn (txn-read conn)]
     (entity-by-id* conn txn eid)))

  ([^Connection conn ^Txn txn eid]
   (let [xf (map (fn [[_ attr value _ _]] [(get-key (.-ident conn) txn (str attr)) value]))]
     (into {:db/id eid} xf (into [] (scan-key (.-eavt-current conn) txn eid))))))

(def entity-by-id (memoize entity-by-id*))

(defn get-and-inc
  [^Connection conn ^Txn txn key]
  (let [eid (or (get-key (.-status conn) txn key) 0)]
    (put-key (.-status conn) txn key (inc eid))
    (inc eid)))

(defn data->datoms
  [^Connection conn ^Txn txn ^Integer tid data]
  (cond
    (map? data)
    (let [[eid entity] (if-let [eid (:db/id data)]
                         [eid (entity-by-id conn txn eid)]
                         [(get-and-inc conn txn :last-eid) nil])
          xf (comp
               (filter (fn [[attr value]]
                         (and (not= attr :db/id)
                              (not= value (get entity attr)))))
               (mapcat (fn [[attr value]]
                         (when (= :db/ident attr)
                           (put-key (.-ident conn) txn value eid)
                           (put-key (.-ident conn) txn (str eid) value))

                         (if-let [ident (get-key (.-ident conn) txn attr)]
                           (if-let [old-value (get entity attr)]
                             [[eid ident old-value tid false]
                              [eid ident value tid true]]
                             [[eid ident value tid true]])
                           (throw (ex-info "ident not found" {:ident attr}))))))]
      (eduction xf data))

    (vector? data)
    (let [[action eid attr value] data]
      (case action
        :db.fn/retractEntity
        (if-let [entity (entity-by-id conn txn eid)]
          (eduction (filter (fn [[attr value]] [eid attr value tid false])) entity)
          (throw (ex-info "entity not found" {:eid eid})))

        :db/add
        [eid attr value true]

        :db/retract
        [eid attr value false]))))

(defn find-datom
  [^Connection conn ^Txn txn [eid attr value]]
  (let [xf (comp
             (filter (fn [[ceid cattr cvalue]] (and (= ceid eid) (= cattr attr) (= cvalue value))))
             (halt-when any?))]
    (transduce xf identity nil (scan-key (.-eavt-current conn) txn eid))))

(defn update-indexes
  [^Connection conn ^Txn txn [eid attr _ _ op :as datom]]
  (let [schema (entity-by-id conn txn attr)]
    (if op
      (do
        (put-key (.-eavt-current conn) txn eid datom)
        (put-key (.-aevt-current conn) txn attr datom))

      (let [[reid rattr :as to-retract] (find-datom conn txn datom)]
        (put-key (.-eavt-history conn) txn eid datom)
        (put-key (.-aevt-history conn) txn attr datom)

        (del-kv (.-eavt-current conn) txn reid to-retract)
        (del-kv (.-aevt-current conn) txn rattr to-retract)))))

(defn transact
  [^Connection conn data]
  (let [tx-data (:tx-data data)]
    (with-open [txn (txn-write conn)]
      (let [tid (System/currentTimeMillis)]
        (let [xf (comp
                   (mapcat (partial data->datoms conn txn tid))
                   (map (partial update-indexes conn txn)))]
          (into [] xf tx-data)
          (txn-commit txn))))))

(defn show-db
  [{:keys [eavt-current eavt-history aevt-current aevt-history status ident] :as conn}]
  (with-open [txn (txn-read conn)]
    {:eavt   {:current (scan-all eavt-current txn true)
              :history (scan-all eavt-history txn true)}
     :aevt   {:current (scan-all aevt-current txn true)
              :history (scan-all aevt-history txn true)}
     :status (scan-all status txn)
     :ident  (scan-all ident txn)}))

;; query

(defn is-binding-var
  [x]
  (when (instance? Named x)
    (.startsWith (name x) "?")))

(defn load-datoms
  [^Connection conn ^Txn txn [eid attr val]]
  (let [filter-attr (filter (fn [[_ dattr _]] (= dattr attr)))
        filter-val (filter (fn [[_ _ dval]] (= dval val)))]
    (cond
      (not (is-binding-var eid))
      (cond
        (and (not (is-binding-var attr))
             (not (is-binding-var val)))
        (eduction filter-attr
                  filter-val
                  (scan-key (.-eavt-current conn) txn eid))

        (not (is-binding-var attr))
        (eduction filter-attr
                  (scan-key (.-eavt-current conn) txn eid))

        (not (is-binding-var val))
        (eduction filter-val
                  (scan-key (.-eavt-current conn) txn eid))

        :else
        (scan-key (.-eavt-current conn) txn eid))

      (not (is-binding-var attr))
      (if-not (is-binding-var val)
        (eduction filter-attr
                  (scan-key (.-aevt-current conn) txn attr))
        (scan-key (.-aevt-current conn) txn attr))

      :else
      (if-not (is-binding-var val)
        (eduction
          (mapcat second)
          filter-val
          (scan-all (.-aevt-current conn) txn))

        (eduction
          (mapcat second)
          (scan-all (.-aevt-current conn) txn))))))

(defn match-datom
  [frame pattern datom]
  (loop [i 0
         frame frame]
    (if (= i (count pattern))
      frame
      (let [binding-var (nth pattern i)
            binded-value (get frame binding-var binding-var)
            datom-value (nth datom i)]
        (if (is-binding-var binding-var)
          (recur (inc i) (assoc frame binding-var datom-value))
          (when (= datom-value binded-value)
            (recur (inc i) frame)))))))

(defn match-pattern
  [frame pattern]
  (comp
    (map (partial match-datom frame pattern))
    (filter (comp not nil?))))

(defn update-pattern
  [conn txn frame [eid attr value]]
  (letfn [(unify [var] (get frame var var))
          (to-ident [var] (get-key (.-ident conn) txn var))]
    [(unify eid) (to-ident (unify attr)) (unify value)]))

(defn entities-by-id [conn eids]
  (map (fn [[eid]] (entity-by-id conn eid)) eids))

(defn q
  [^Connection conn {:keys [find where]}]
  (letfn [(reducing-fn
            [txn frames pattern]
            (let [xf (mapcat (fn [frame]
                               (let [pattern (update-pattern conn txn frame pattern)
                                     datoms (load-datoms conn txn pattern)]
                                 (eduction (match-pattern frame pattern) datoms))))]
              (into [] xf frames)))]

    (let [frames (with-open [txn (txn-read conn)]
                   (reduce (partial reducing-fn txn) [{}] where))]
      (for [frame frames]
        (for [f find]
          (get frame f))))))