# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name = %q{kafka-rb}
  s.version = "0.0.11"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Alejandro Crosa", "Stefan Mees", "Tim Lossen", "Liam Stewart"]
  s.autorequire = %q{kafka-rb}
  s.date = Time.now.strftime("%Y-%m-%d")
  s.description = %q{kafka-rb allows you to produce and consume messages using the Kafka distributed publish/subscribe messaging service.}
  s.extra_rdoc_files = ["LICENSE"]
  s.files = ["LICENSE", "README.md", "Rakefile"] + Dir.glob("lib/**/*.rb")
  s.test_files = Dir.glob("spec/**/*.rb")
  s.homepage = %q{http://github.com/acrosa/kafka-rb}
  s.require_paths = ["lib"]
  s.summary = %q{A Ruby client for the Kafka distributed publish/subscribe messaging service}

  s.add_development_dependency(%q<rspec>, [">= 0"])
  s.add_development_dependency(%q<rake>, [">= 0"])
end
