package org.improving.workshop.project

import spock.lang.Specification


class ConnecticutFavoriteArtistSpec extends Specification {
  def "example test"() {
    given:
    def instance = new ConnecticutFavoriteArtist()

    expect:
    instance != null
  }
}