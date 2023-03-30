object GeohashUtil {
  def getGeohashQuadrants(geohash: String): List[List[String]] = {
    val subdivision = List(
      List("b", "c", "f", "g", "u", "v", "y", "z"),
      List("8", "9", "d", "e", "s", "t", "w", "x"),
      List("2", "3", "6", "7", "k", "m", "q", "r"),
      List("0", "1", "4", "5", "h", "j", "n", "p")
    )

    val quadrants = for {
      row <- subdivision.indices by 2
      col <- subdivision(row).indices by 2
    } yield
      List(
        geohash + subdivision(row)(col),
        geohash + subdivision(row)(col + 1),
        geohash + subdivision(row + 1)(col),
        geohash + subdivision(row + 1)(col + 1)
      )

    quadrants.toList
  }
}
