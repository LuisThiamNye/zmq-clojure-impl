(ns crypticbutter.zmq.impl.socket.base
  (:require
   [clojure.string :as str]
   [farolero.core :as f :refer [signal error warn]]
   [taoensso.encore :as enc]
   [crypticbutter.snoop :refer [>defn => >defn-]]
   [cyrpticbutter.zmq.impl.msg :as msg])
  (:import
   (java.nio.channels ServerSocketChannel)
   (java.net InetAddress
             StandardSocketOptions)))

(def windows? (str/starts-with? (System/getProperty "os.name")
                                "Windows"))


(derive ::error.invalid-endpoint :error/incorrect)
(derive ::error.unsupported-protocol :error/unsupported)
(derive ::error.incompatible-protocol :error/unsupported)
(derive ::error.address-in-use :error/conflict)
(derive ::error.address-not-local :error/unsupported) ;; ADDRNOTAVAIL The requested address was not local.
(derive ::error.device-not-found :error/unavailable)
(derive ::error.context-terminated :error/conflict)
(derive ::error.invalid-socket :error/incorrect)
(derive ::error.no-io-thread :error/unavailable)

  ;; unavailable (server not there)
  ;; interrupted
  ;; incorrect
  ;; forbidden
  ;; unsupported
  ;; not-found (resource)
  ;; conflict (with state of system)
  ;; fault (something went wrong that we do not know)
  ;; busy

(def tcp-min-dyn-port 49152)
(def tcp-max-dyn-port 65535)

(defn bind-to-random-port!
  ([socket endpoint]
   (bind-to-random-port!
    socket endpoint tcp-min-dyn-port tcp-max-dyn-port))
  ([socket endpoint min-port max-port]
   (loop [port min-port]
     (if (<= port max-port)
       (if (try (.bind socket (str endpoint ":" port))
                (catch Exception _ false))
         true
         (recur (inc port)))
       false))))

(defn connect
  [sock endpoint])

(defn parse-tcp-auth [authority ipv6?]
  (let [[_ input-host input-port] (re-matches #"(.+?)(?::(\d+))" authority)]
    [(if (identical? "*" input-host)
       (if ipv6?
         "::"
         "0.0.0.0")
       input-host)
     (if (or (nil? input-port)
             (identical? "*" input-port))
       0
       (Integer/parseInt input-port))]))

(>defn- socket-address
  [host port ipv6?]
  [string? nat-int? boolean? => :any]
  (try
    (let [ip-addrs (.getAllByName InetAddress host)
          chosen-ip-addr (if ipv6?
                           (or (some #(instance? java.net.Inet6Address %) ip-addrs)
                               (first ip-addrs))
                           (some #(instance? java.net.Inet4Address %) ip-addrs))]
      (if (some? chosen-ip-addr)
        (InetAddress. chosen-ip-addr port)
        (error ::error.address-not-local
               {:message (str host " not available for ipv6 enable state: " ipv6?)})))
    (catch java.net.UnknownHostException e
      (error ::error.address-not-local
             {:message (.getMessage e)}))))



(>defn bind-tcp
  [*ctx sock (authority string?)]
  (let [ctx @*ctx]
    (if-some [io-thread (choose-io-thread (vals (:ctx/io-threads ctx)))]
      (let [sock-chan (.open ServerSocketChannel)]
        (try
          (let [ipv6? (::ipv6-enabled sock)
                [host port] (parse-tcp-auth authority ipv6?)
                snd-buf (::send-buffer-size sock)
                rcv-buf (::recv-buffer-size sock)
                sock-chan (doto sock-chan
                            (.configureBlocking  false)
                            (cond-> (some? snd-buf)
                              (.setOption (.-SO_SNDBUF StandardSocketOptions)
                                          snd-buf))
                            (cond-> (some? rcv-buf)
                              (.setOption (.-SO_SNDBUF StandardSocketOptions)
                                          rcv-buf))
                            (cond-> windows?
                              (.setOption (.-SO_REUSEADDR StandardSocketOptions)
                                          true)))]
            (.bind (.socket sock-chan)
                   (socket-address host port ipv6?)
                   (::max-backlog sock))
            (assoc sock ::socket-chan sock-chan))
          (catch java.io.IOException _
            (try (.close sock-chan)
                 (catch java.io.IOException _
                         ;; TODO
                   ))
            (error ::error.address-in-use
                   {:message "The requested address is already in use."}))))
      (error ::error.no-io-thread
             {:message "No I/O thread is available to accomplish the task."}))))

(defn bind
  "Accept incoming connections on a socket.

  Endpoint must be local."
  [*ctx sock endpoint]
  (let [[_ scheme authority]
        (re-matches #"(\w+)://(.+)" endpoint)]
    (when false ;; TODO
      (error ::error.context-terminated
             {:message "The ØMQ context associated with the specified socket was terminated."}))
    (case scheme
      "inproc" (do
                 ;; TODO register endpoint
                 (error ::error.address-in-use
                        {:message "The requested address is already in use."}))
      "tcp" (bind-tcp *ctx sock authority)
      (error ::error.unsupported-protocol
             {:message "The requested transport protocol is not supported."}))))

(defn close
  [sock]
  (.close sock))

(defn socket
  "Creates a socket"
  ([ctx sock-type] (socket ctx sock-type {}))
  ([ctx sock-type {:keys [bind connect subscribe]}]

   (.open ServerSocketChannel)))
