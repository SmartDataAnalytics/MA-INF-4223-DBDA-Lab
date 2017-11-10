package foil.utils

/**
 * Just testing some vectors
 *
 * Created by m4b on 4/29/14.
 */
class Vec3(var x: Int, var y: Int, var z: Int) {

  def set(x: Int, y: Int, z: Int): Vec3 = {
    this.x = x; this.y = y; this.z = z; this
  }

  def crs(that: Vec3):Vec3 = {
    set((this.y * that.z) - (this.z * that.y),
      (this.z * that.x) - (this.x * that.z),
      (this.x * that.y) - (this.y * that.x))
    this
  }

  def len = Math.sqrt(this.x^2 + this.y^2 + this.z^2)

  override def toString = "<" + this.x + ", " + this.y + ", " + this.z + ">"

}
