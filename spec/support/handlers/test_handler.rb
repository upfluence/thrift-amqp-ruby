class TestHandler
  def foo(x, y)
    x + y
  end

  def bar(x, y)
    f = File.open('/tmp/test-rspec', 'w')
    f << x + y
    f.flush
    f.close
  end
end
