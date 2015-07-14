require 'spec_helper'


describe 'Client' do
  before(:all) do
    @pid = fork do
      run_server
    end
  end

  after(:all) do
    Process.kill "TERM", @pid
  end

  let!(:client) { client, @trans = build_client; client }

  before(:each) do
    @trans.open
  end

  after(:each) do
    @trans.close
  end

  it do
    client.bar(1, 2)
    sleep 0.5
    expect(File.read('/tmp/test-rspec')).to eq("3")
  end

  it do
    expect(client.foo(1, 2)).to eq(3)
  end
end
