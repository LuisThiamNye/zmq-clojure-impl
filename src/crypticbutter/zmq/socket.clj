(ns crypticbutter.zmq.socket
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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment
  {::affinity nil ;; TODO thread numbers to start new sockets on (eg set)
   ::max-backlog 100
   ::device-to-bind nil
   ;; If set, a socket shall keep only one message in its inbound/outbound queue, this message being the last message received/the last message to be sent. Ignores ZMQ_RCVHWM and ZMQ_SNDHWM options. Does not support multi-part messages, in particular, only one part of it is kept in the socket internal queue.
   ::conflate? false

   ::connection-routing-id nil
   ::connection-timeout nil

   ::curve-public-key nil
   ::curve-secret-key nil
   ::curve-server? false
   ::curve-server-key nil

   ::max-handshake-interval 30000 ;;ms
   ::heartbeat-interval nil
   ::heartbeat-timeout nil
   ::heartbeat-ttl nil

   ::only-queue-connected? false ;; IMMEDIATE
   ::invert-message-filtering false ;; INVERT_MATCHING
   ::ipv6-enabled false
   ::linger-duration nil
   ::max-msg-size nil
   ::metadata nil

   ::multicast-max-hops 1
   ::multicast-max-tpdu 1500 ;; ZMQ_MULTICAST_MAXTPDU bytes
   ::multicast-data-rate 100 ;; kbit/s
   ::multicast-recovery-interval 10000 ;;ms
   ::multicast-loopback? true

   ::plain-password nil
   ::plain-server? false
   ::plain-username nil

   ::file-descriptor nil ;; USE FD
   ::probe-router? false

   ::recv-buffer-size nil
   ::recv-msg-limit 1000 ;; RCVHWM
   ::recv-timeout nil

   ::reconnection-interval 100 ;; ms
   ::reconnection-max-interval nil

   ;; ::req-correlate? false
   ;; ::req-strict-alternation? true  ;; if false, user should also enable req-correlate
   ::req-matching-rule :alternation ;; or :frames

   ::router-handover? false
   ::router-reject-unroutable? false ;; ROUTER_MANDATORY
   ::router-raw-mode? false
   ::router-notify? false

   ::routing-id nil

   ::send-buffer-size nil
   ::send-msg-limit 1000 ;; SNDHWM
   ::send-timeout nil

   ::socks-proxy-addr nil

   ::stream-notify? true

   ::subscribe-prefix ""

   ::tcp-keepalive nil
   ::tcp-keepalive-cnt nil
   ::tcp-keepalive-idle nil
   ::tcp-keepalive-interval nil
   ::tcp-maxrt nil
   ::tos 0

   ::xpub-verbosity 0
   ::xpub-manual? false
   ::xpub-no-drop? false
   ::xpub-welcome-msg nil

;;
   }
  (def j (atom 5))

  (.start (Thread.
           (fn []
             (swap! j inc))))

  (deref j)

  (require '[clojure.pprint :as pp])

  (pp/print-table
   (for [af (range 1 5)]
     {:affinity (Integer/toBinaryString af)
      :threads
      (filterv #(pos? (bit-and
                       af
                       (bit-shift-left
                        1 %)))
               (range 0 6))}))
  ;;
  )
