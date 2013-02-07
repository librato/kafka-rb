# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  module IO
    attr_accessor :socket, :hostlist, :hostidx, :timeout, :retries

    PORT = 9092
    TIMEOUT = 10
    RETRIES = 1

    # How long before we retry a connection to a server
    DEAD_SERVER_SECS = 30

    def connect(hosts, timeout = TIMEOUT)
      raise ArgumentError, "No hosts specified" unless hosts
      if hosts.class == String
        hosts = [hosts]
      end

      self.hostlist = []
      hosts.each do |host|
        h = {}
        sp = host.split(":")
        h[:hostname] = sp[0]
        h[:port] = sp.length > 1 ? sp[1].to_i : PORT
        h[:last_connect] = nil
        self.hostlist << h
      end

      # XXX: Should order be based on some node hash? Want to
      # protect against thundering herd
      #
      self.hostlist.shuffle!

      self.timeout = timeout.to_i
      self.retries = RETRIES
      self.hostidx = 0
      self.reconnect()
    end

    def curr_host
      self.hostlist[self.hostidx]
    end

    def reconnect
      begin
        curr_host[:last_connect] = Time.now.tv_sec
        self.socket = open(curr_host)
      rescue
        # Rotate through the hostlist. Once we've attempted to connect
        # to each server within DEAD_SERVER_SECS, finally raise the
        # error.
        self.disconnect
        self.hostidx = (self.hostidx + 1) % self.hostlist.length
        now = Time.now.tv_sec
        if curr_host[:last_connect].nil? ||
            curr_host[:last_connect] + DEAD_SERVER_SECS <= now
          retry
        else
          raise
        end
      end
    end

    # Open socket connection, block up till self.timeout secs
    def open(host)
      addr = Socket.getaddrinfo(host[:hostname], nil)
      sock = Socket.new(Socket.const_get(addr[0][0]),
                        Socket::SOCK_STREAM, 0)

      begin
        inaddr = Socket.pack_sockaddr_in(host[:port], addr[0][3])
        sock.connect_nonblock(inaddr)
      rescue Errno::EINPROGRESS
        resp = ::IO.select(nil, [sock], nil, self.timeout)
        if resp.nil?
          raise Errno::ECONNREFUSED
        end
        begin
          sock.connect_nonblock(inaddr)
        rescue Errno::EISCONN
        end
      end
      sock
    end

    def disconnect
      self.socket.close rescue nil
      self.socket = nil
    end

    def read(length)
      self.socket.read(length) || raise(SocketError, "no data")
    rescue
      self.disconnect
      raise SocketError, "cannot read: #{$!.message}"
    end

    def write(data)
      self.reconnect unless self.socket

      tries = 0
      total = 0
      len = data.length
      while total < len
        begin
          tries += 1
          result = self.socket.write(data[total, len])
          if result > 0
            total += result
          elsif result == 0
            break
          end
        rescue ::IO::WaitWritable, Errno::EINTR
          ::IO.select(nil, [self.socket], nil, self.timeout)
          if tries <= retries
            retry
          else
            raise
          end
        rescue Errno::EPIPE, Errno::ECONNRESET, Errno::ETIMEDOUT
          if tries <= retries
            # Try to reconnect
            self.disconnect
            self.reconnect

            # Reset data pointer so that we don't write
            # mid-way through a message on reconnect
            total = 0
            retry
          else
            raise
          end
        end
      end
    rescue => err
      self.disconnect
      raise SocketError.new("cannot write: #{$!.message}", err)
    end

  end
end
