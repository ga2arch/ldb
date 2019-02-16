(ns ldb.db
  (:require [clojure.data.fressian :as fress]
            [clojure.spec.alpha :as s]
            [ldb.thread :refer [thread-local]])
  (:import (org.lmdbjava Env DbiFlags PutFlags KeyRange CursorIterator Dbi Txn)
           (java.io File)
           (java.nio ByteBuffer)
           (clojure.lang IReduceInit Named)))

(defrecord Connection [^Dbi eavt-current ^Dbi eavt-history
                       ^Dbi aevt-current ^Dbi aevt-history
                       ^Dbi avet-current ^Dbi avet-history
                       ^Dbi vaet-current ^Dbi vaet-history
                       ^Env env ^Dbi status ^Dbi ident])

;; specs

(s/def :db/ident keyword?)
(s/def :db/valueType #{:db.type/string :db.type/keyword :db.type/long :db.type/ref})
(s/def :db/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality]))

;; const

(def schema-idents [:db/ident :db/valueType :db/cardinality
                    :db.type/keyword :db.type/string :db.type/long :db.type/ref
                    :db.cardinality/one :db.cardinality/many])

;;

(def bkey (thread-local (ByteBuffer/allocateDirect 511)))
(def bval (thread-local (ByteBuffer/allocateDirect 2000)))

(defn encode-key
  [data]
  (let [bkey @bkey]
    (.clear bkey)
    (cond
      (int? data)
      (.putInt bkey data)
      :else
      (.put bkey (fress/write data)))
    (.flip bkey)))

(defn encode-val
  [data]
  (doto @bval
    (.clear)
    (.put (fress/write data))
    (.flip)))

(defn decode
  [data]
  (fress/read data))

(defn decode-key
  [buffer int-key?]
  (if int-key?
    (.getInt buffer)
    (decode buffer)))

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

(defn scan-all
  ([^Dbi db ^Txn txn]
   (scan-all db txn false))
  ([^Dbi db ^Txn txn int-key?]
   (let [^CursorIterator it (.iterate db txn (KeyRange/all))]
     (let [vals (volatile! {})]
       (while (.hasNext it)
         (let [kv (.next it)
               k (decode-key (.key kv) int-key?)
               v (decode (.val kv))]
           (vswap! vals (fn [vals] (update vals k (fn [a] ((fnil conj []) a v)))))))
       @vals))))

(defn del-key
  [^Dbi db ^Txn txn key]
  (.delete db txn (encode-key key)))

(defn del-kv
  [^Dbi db ^Txn txn key val]
  (.delete db txn (encode-key key) (encode-val val)))

(defn close
  [^Connection conn]
  (.close ^Env (.-env conn)))

(defn insert-ident
  [^Connection conn ^Txn txn key value]
  (put-key (.-ident conn) txn key value)
  (put-key (.-ident conn) txn (str value) key))

(defn from-ident
  [^Connection conn ^Txn txn key]
  (get-key (.-ident conn) txn (str key)))

(defn to-ident
  [^Connection conn ^Txn txn key]
  (get-key (.-ident conn) txn key))

(defn connect
  [^String filepath]
  (let [^Env env (-> (Env/create)
                     (.setMapSize 1099511627776)
                     (.setMaxDbs 7)
                     (.open (File. filepath) nil))]
    (letfn [(open-db [name & flags]
              (.openDbi env name (into-array DbiFlags flags)))]
      (let [conn (map->Connection {:env          env
                                   :eavt-current (open-db "eavt-current" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                                   :eavt-history (open-db "eavt-history" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                                   :aevt-current (open-db "aevt-current" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                                   :aevt-history (open-db "aevt-history" DbiFlags/MDB_CREATE DbiFlags/MDB_DUPSORT DbiFlags/MDB_INTEGERKEY)
                                   :status       (open-db "status" DbiFlags/MDB_CREATE)
                                   :ident        (open-db "ident" DbiFlags/MDB_CREATE)})]
        (with-open [txn (txn-write conn)]
          (doseq [[i ident] (map-indexed vector schema-idents)]
            (insert-ident conn txn ident (- (inc i))))
          (txn-commit txn))
        conn))))
;;

(defn ^:dynamic entity-by-id
  ([^Connection conn eid]
   (with-open [txn (txn-read conn)]
     (entity-by-id conn txn eid)))
  ([^Connection conn ^Txn txn eid]
   (let [xf (map (fn [[_ attr value _ _]] [(from-ident conn txn attr) value]))]
     (into {:db/id eid} xf (into [] (scan-key (.-eavt-current conn) txn eid))))))

(defn get-and-inc
  [^Connection conn ^Txn txn key]
  (let [eid (or (get-key (.-status conn) txn key) -1)]
    (put-key (.-status conn) txn key (inc eid))
    (inc eid)))

(defn valid-type?
  [valueType value]
  (case valueType
    :db.type/long
    (int? value)

    :db.type/string
    (string? value)

    :db.type/keyword
    (keyword? value)

    :db.type/ref
    (map? value)

    false))

(defn valid-value?
  [{:db/keys [_ valueType cardinality]} value]
  (case cardinality
    :db.cardinality/one
    (valid-type? valueType value)

    :db.cardinality/many
    (and (coll? value)
         (every? (partial valid-type? valueType) value))))

(defn validate-value
  [value schema]
  (when-not (valid-value? schema value)
    (throw (ex-info "invalid value" {:value  value
                                     :schema schema}))))

(defn map->datoms
  [^Connection conn ^Txn txn ^Integer tid data]
  (when (:db/ident data)
    (s/explain ::schema data))

  (let [[eid entity] (if-let [eid (:db/id data)]
                       [eid (entity-by-id conn txn eid)]
                       [(get-and-inc conn txn :last-eid) nil])]
    (letfn [(filter-attr [[attr value]]
              (and (not= attr :db/id)
                   (not= value (get entity attr))))

            (update-ident [[attr value]]
              (when (= :db/ident attr)
                (insert-ident conn txn value eid))
              (if-let [ident (to-ident conn txn attr)]
                [ident attr value]
                (throw (ex-info "ident not found" {:attr  attr
                                                   :value value}))))

            (validate [[ident _ value :as all]]
              (when-not (neg? ident)
                (let [schema (entity-by-id conn txn ident)]
                  (validate-value value schema)))
              all)

            (to->datoms [[ident attr value]]
              (if (neg? ident)
                [[eid ident value tid true]]
                (let [value (if (coll? value) (set value) value)
                      old-value (get entity attr)
                      {:db/keys [valueType cardinality]} (entity-by-id conn txn ident)]

                  (case cardinality
                    :db.cardinality/one
                    (case valueType
                      :db.type/ref
                      (let [ref-datom (map->datoms conn txn tid value)
                            id (ffirst ref-datom)
                            datoms (if old-value
                                     [[eid ident old-value tid false]
                                      [eid ident id tid true]]
                                     [[eid ident id tid true]])]
                        (concat datoms ref-datom))

                      (if old-value
                        [[eid ident old-value tid false]
                         [eid ident value tid true]]
                        [[eid ident value tid true]]))

                    :db.cardinality/many
                    (case valueType
                      :db.type/ref
                      (let [ref-datoms (mapcat (partial map->datoms conn txn tid) value)
                            ids (into #{} (map first) ref-datoms)
                            datoms (if old-value
                                     [[eid ident old-value tid false]
                                      [eid ident (into ids old-value) tid true]]
                                     [[eid ident ids tid true]])]
                        (concat ref-datoms datoms))

                      (if old-value
                        [[eid ident old-value tid false]
                         [eid ident (into value old-value) tid true]]
                        [[eid ident value tid true]]))))))]

      (eduction (filter filter-attr)
                (map update-ident)
                (map validate)
                (mapcat to->datoms) data))))

(defn data->datoms
  [^Connection conn ^Txn txn ^Integer tid data]
  (cond
    (map? data)
    (map->datoms conn txn tid data)

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
  (if op
    (do
      (put-key (.-eavt-current conn) txn eid datom)
      (put-key (.-aevt-current conn) txn attr datom))

    (let [[reid rattr :as to-retract] (find-datom conn txn datom)]
      (put-key (.-eavt-history conn) txn eid datom)
      (put-key (.-aevt-history conn) txn attr datom)

      (del-kv (.-eavt-current conn) txn reid to-retract)
      (del-kv (.-aevt-current conn) txn rattr to-retract))))

(defn transact
  [^Connection conn data]
  (let [tx-data (:tx-data data)]
    (with-open [txn (txn-write conn)]
      (let [tid (System/currentTimeMillis)]
        (binding [entity-by-id (memoize entity-by-id)]
          (let [xf (comp
                     (mapcat (partial data->datoms conn txn tid))
                     (map (partial update-indexes conn txn)))]
            (into [] xf tx-data)))
        (txn-commit txn)))))

(defn show-db
  [{:keys [eavt-current eavt-history aevt-current aevt-history status ident] :as conn}]
  (with-open [txn (txn-read conn)]
    {:eavt   {:current (into (sorted-map) (scan-all eavt-current txn true))
              :history (into (sorted-map) (scan-all eavt-history txn true))}
     :aevt   {:current (into (sorted-map) (scan-all aevt-current txn true))
              :history (into (sorted-map) (scan-all aevt-history txn true))}
     :status (scan-all status txn)
     :ident  (scan-all ident txn)}))

;; query

(defn binding-var?
  [x]
  (when (instance? Named x)
    (.startsWith (name x) "?")))

(defn load-datoms
  [^Connection conn ^Txn txn [eid attr val]]
  (let [filter-attr (filter (fn [[_ dattr _]] (= dattr attr)))
        filter-val (filter (fn [[_ _ dval]] (= dval val)))]
    (cond
      (not (binding-var? eid))
      (cond
        (and (not (binding-var? attr))
             (not (binding-var? val)))
        (eduction filter-attr
                  filter-val
                  (scan-key (.-eavt-current conn) txn eid))

        (not (binding-var? attr))
        (eduction filter-attr
                  (scan-key (.-eavt-current conn) txn eid))

        (not (binding-var? val))
        (eduction filter-val
                  (scan-key (.-eavt-current conn) txn eid))

        :else
        (scan-key (.-eavt-current conn) txn eid))

      (not (binding-var? attr))
      (if-not (binding-var? val)
        (eduction filter-attr
                  (scan-key (.-aevt-current conn) txn attr))
        (scan-key (.-aevt-current conn) txn attr))

      :else
      (if-not (binding-var? val)
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
        (if (binding-var? binding-var)
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
  (letfn [(unify [var] (get frame var var))]
    [(unify eid) (to-ident conn txn (unify attr)) (unify value)]))

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