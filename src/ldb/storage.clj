(ns ldb.storage
  (:require [clojure.reflect :refer [reflect]])
  (:import (sun.misc Unsafe)
           (sun.nio.ch FileChannelImpl)
           (java.nio.file OpenOption StandardOpenOption Paths)
           (java.io File)))

(defn- get-java-method
  [clazz name & args]
  (doto
    (.getDeclaredMethod clazz name (into-array Class args))
    (.setAccessible true)))

(def unsafe (-> (doto
                  (.getDeclaredField Unsafe "theUnsafe")
                  (.setAccessible true))
                (.get nil)))
(def byte-array-offset (.arrayBaseOffset unsafe (Class/forName "[B")))
(def map0 (get-java-method FileChannelImpl "map0" Integer/TYPE Long/TYPE Long/TYPE))
(def unmap0 (get-java-method FileChannelImpl "unmap0" Long/TYPE Long/TYPE))

(defn mmap
  [^String loc ^Long size]
  (let [size (bit-and (+ size 0xfff) (bit-not 0xfff))]
    (with-open [ch (FileChannelImpl/open
                     (Paths/get loc (into-array String []))
                     (into-array OpenOption [StandardOpenOption/READ
                                             StandardOpenOption/SPARSE
                                             StandardOpenOption/CREATE]))]
      (long (.invoke map0 ch (into-array Object [(int 0) (long 0) (long size)]))))))

(defn open-file
  [^String loc]
  (FileChannelImpl/open
    (.toPath (File. loc))
    (into-array OpenOption [StandardOpenOption/APPEND
                            StandardOpenOption/SPARSE
                            StandardOpenOption/CREATE
                            StandardOpenOption/DSYNC])))

(defn read-bytes
  [addr offset length]
  (let [data (byte-array length)]
    (.copyMemory unsafe nil (+ addr offset) data byte-array-offset length)
    data))

(comment
  (clojure.java.shell/sh "cat" "tmp")

  (def addr (mmap "tmp" 0 0 10))

  (String. (read-bytes addr 0 10))

  (clojure.java.shell/sh "cat" "tmp"))