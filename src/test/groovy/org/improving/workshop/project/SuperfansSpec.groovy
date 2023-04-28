package org.improving.workshop.project

import spock.lang.Specification


class SuperfansSpec extends Specification {
  def "example test"() {
    given:
    def instance = new Superfans()

    expect:
    instance != null
  }
}