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
require File.dirname(__FILE__) + '/spec_helper'

describe Consumer do

  before(:each) do
    @mocked_socket = mock(TCPSocket)
    TCPSocket.stub!(:new).and_return(@mocked_socket) # don't use a real socket
    @consumer = Consumer.new(:offset => 0)
  end

  describe "Kafka Consumer" do

    it "should have a Kafka::RequestType::FETCH" do
      Kafka7::RequestType::FETCH.should eql(1)
      @consumer.should respond_to(:request_type)
    end

    it "should have a topic and a partition" do
      @consumer.should respond_to(:topic)
      @consumer.should respond_to(:partition)
    end

    it "should have a polling option, and a default value" do
      Consumer::DEFAULT_POLLING_INTERVAL.should eql(2)
      @consumer.should respond_to(:polling)
      @consumer.polling.should eql(2)
    end

    it "should set a topic and partition on initialize" do
      @consumer = Consumer.new({ :host => "localhost", :port => 9092, :topic => "testing" })
      @consumer.topic.should eql("testing")
      @consumer.partition.should eql(0)
      @consumer = Consumer.new({ :topic => "testing", :partition => 3 })
      @consumer.partition.should eql(3)
    end

    it "should set default host and port if none is specified" do
      @consumer = Consumer.new
      @consumer.host.should eql("localhost")
      @consumer.port.should eql(9092)
    end

    it "should not have a default offset but be able to set it" do
      @consumer = Consumer.new
      @consumer.offset.should be_nil
      @consumer = Consumer.new({ :offset => 1111 })
      @consumer.offset.should eql(1111)
    end

    it "should have a max size" do
      Consumer::MAX_SIZE.should eql(1048576)
      @consumer.max_size.should eql(1048576)
    end

    it "should return the size of the request" do
      @consumer.topic = "someothertopicname"
      @consumer.encoded_request_size.should eql([38].pack("N"))
    end

    it "should encode a request to consume" do
      bytes = [Kafka7::RequestType::FETCH].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [0].pack("q").reverse + [Kafka7::Consumer::MAX_SIZE].pack("N")
      @consumer.encode_request(Kafka7::RequestType::FETCH, "test", 0, 0, Kafka7::Consumer::MAX_SIZE).should eql(bytes)
    end

    it "should read the response data" do
      bytes = [0].pack("n") + [1120192889].pack("N") + "ale"
      @mocked_socket.should_receive(:read).and_return([9].pack("N"))
      @mocked_socket.should_receive(:read).with(9).and_return(bytes)
      @consumer.read_data_response.should eql(bytes[2,7])
    end

    it "should send a consumer request" do
      @consumer.stub!(:encoded_request_size).and_return(666)
      @consumer.stub!(:encode_request).and_return("someencodedrequest")
      @consumer.should_receive(:write).with("someencodedrequest").exactly(:once).and_return(true)
      @consumer.should_receive(:write).with(666).exactly(:once).and_return(true)
      @consumer.send_consume_request.should eql(true)
    end

    it "should consume messages" do
      @consumer.should_receive(:send_consume_request).and_return(true)
      @consumer.should_receive(:read_data_response).and_return("")
      @consumer.consume.should eql([])
    end

    it "should loop and execute a block with the consumed messages" do
      @consumer.stub!(:consume).and_return([mock(Kafka7::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:once).and_return([])
      @consumer.loop do |message|
        messages << message
        break # we don't wanna loop forever on the test
      end
    end

    it "should loop (every N seconds, configurable on polling attribute), and execute a block with the consumed messages" do
      @consumer = Consumer.new({ :polling => 1 })
      @consumer.stub!(:consume).and_return([mock(Kafka7::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:twice).and_return([])
      executed_times = 0
      @consumer.loop do |message|
        messages << message
        executed_times += 1
        break if executed_times >= 2 # we don't wanna loop forever on the test, only 2 seconds
      end

      executed_times.should eql(2)
    end

    it "should fetch initial offset if no offset is given" do
      @consumer = Consumer.new
      @consumer.should_receive(:fetch_latest_offset).exactly(:once).and_return(1000)
      @consumer.should_receive(:send_consume_request).and_return(true)
      @consumer.should_receive(:read_data_response).and_return("")
      @consumer.consume
      @consumer.offset.should eql(1000)
    end

    it "should encode an offset request" do
      bytes = [Kafka7::RequestType::OFFSETS].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [-1].pack("q").reverse + [Kafka7::Consumer::MAX_OFFSETS].pack("N")
      @consumer.encode_request(Kafka7::RequestType::OFFSETS, "test", 0, -1, Kafka7::Consumer::MAX_OFFSETS).should eql(bytes)
    end

    it "should parse an offsets response" do
      bytes = [0].pack("n") + [1].pack('N') + [21346].pack('q').reverse
      @mocked_socket.should_receive(:read).and_return([14].pack("N"))
      @mocked_socket.should_receive(:read).and_return(bytes)
      @consumer.read_offsets_response.should eql(21346)
    end
  end
end
