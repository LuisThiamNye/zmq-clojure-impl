(ns crypticbutter.zmq
  (:require
   [farolero.core :as f :refer [signal error warn]]
   [lambdaisland.uri :as uri]))

(comment

  ;; => (:scheme :user :password :host :port :path :query :fragment)
  (-> (uri/uri "tcp://*:45")
      ;; (assoc :port 8888)
      :path)

  (re-find #"\w" "-")

  ;; transports:
  ;; tcp
  ;; ipc
  ;; inproc
  ;; pgm, epgm
  ;; vmci

  ;; Datomic error categories
  ;; unavailable
  ;; interrupted
  ;; incorrect
  ;; forbidden
  ;; unsupported
  ;; not-found
  ;; conflict (with state of system)
  ;; fault (something went wrong that we do not know)
  ;; busy
  ;;
  )
