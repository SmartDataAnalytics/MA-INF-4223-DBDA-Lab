package blast.data_processing

// a class for duplicates based on their URL (entity url)
class url_duplicates(url1 : String , url2 :String) {
  val url_1 :String = url1
  val url_2 :String = url2

  def is_identical(other : url_duplicates): Boolean ={
    if ( ((this.url_1 == other.url_1) && (this.url_2 == other.url_2))  || ((this.url_1 == other.url_2) && (this.url_2 == other.url_1)) ) return true
    else return false
  }

}
