(ns crypticbutter.zmq.context
  (:require
   [clojure.core.async :as async :refer [go go-loop <! <!! >! >!!]]
   [farolero.core :as f :refer [signal error warn]]
   [taoensso.encore :as enc]
   [crypticbutter.zmq.socket :as sock])
  (:import
   (java.nio.channels SelectableChannel
                      SelectionKey
                      Selector)))



(defn create-context
  ([options]
   (let [opts (enc/merge {::io-thread-count 1
                          ::io-thread-name-prefix "zmq-io-thread-"
                          ::max-sockets 1024
                          ::ipv6-enabled false
                          ::block-on-termination true}
                         options)]
     (atom {:ctx/io-thread-name-prefix (::io-thread-name-prefix opts)
            :ctx/max-sockets (::max-sockets 1024 opts)
            :ctx/ipv6-enabled (::ipv6-enabled opts)
            :ctx/block-on-termination (::block-on-termination opts)
            :ctx/io-threads (into {}
                                  (map (fn [n]
                                         [(keyword (name (::io-thread-name-prefix opts))
                                                   n)
                                          (map->IOThread {:status :init})]))
                                  (range (::io-thread-count opts)))}))))




(comment
  {::io-thread-count 1
   ::io-thread-name-prefix "io-thread-"
   ::max-sockets 1024
   ::ipv6-enabled false
   ::block-on-termination true}

  ;;
  )
