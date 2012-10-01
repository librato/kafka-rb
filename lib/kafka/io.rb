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
    attr_accessor :socket, :host, :port, :timeout, :retries

    HOST = "localhost"
    PORT = 9092
    TIMEOUT = 10
    RETRIES = 1

    def connect(host, port, timeout = TIMEOUT)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.timeout = timeout.to_i
      self.retries = RETRIES
      self.socket = open()
    end

    def reconnect
      self.socket = open()
    rescue
      self.disconnect
      raise
    end

    # Open socket connection, block up till self.timeout secs
    def open
      addr = Socket.getaddrinfo(self.host, nil)
      sock = Socket.new(Socket.const_get(addr[0][0]),
                        Socket::SOCK_STREAM, 0)

      begin
        inaddr = Socket.pack_sockaddr_in(self.port, addr[0][3])
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
          if tries < retries
            retry
          else
            raise
          end
        end
      end
    rescue
      self.disconnect
      raise SocketError, "cannot write: #{$!.message}"
    end

  end
end
