import com.griddynamics.aborgatin.finalproject.NetworkUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class Tests extends FunSuite with BeforeAndAfterEach {



  test("test binary search"){
    val arr = Array((3l,5l, "Russia"),
      (6l,8l, "USA"),
      (9l,16l, "Canada"),
      (17l,20l, "Egipet"),
      (21l,24l, "France"),
      (25l,30l, "England"),
      (31l,45l, "Sweden")
    )

    assert(null == NetworkUtils.getCountry(arr,1l))
    assert("Russia".equals(NetworkUtils.getCountry(arr,3l)))
    assert("Russia".equals(NetworkUtils.getCountry(arr,5l)))
    assert("USA".equals(NetworkUtils.getCountry(arr,6l)))
    assert("USA".equals(NetworkUtils.getCountry(arr,7l)))
    assert("USA".equals(NetworkUtils.getCountry(arr,8l)))
    assert("Canada".equals(NetworkUtils.getCountry(arr,9l)))
    assert("Canada".equals(NetworkUtils.getCountry(arr,13l)))
    assert("Canada".equals(NetworkUtils.getCountry(arr,16l)))
    assert("Egipet".equals(NetworkUtils.getCountry(arr,17l)))
    assert("Egipet".equals(NetworkUtils.getCountry(arr,18l)))
    assert("Egipet".equals(NetworkUtils.getCountry(arr,20l)))
    assert("France".equals(NetworkUtils.getCountry(arr,21l)))
    assert("France".equals(NetworkUtils.getCountry(arr,22l)))
    assert("France".equals(NetworkUtils.getCountry(arr,24l)))
    assert("England".equals(NetworkUtils.getCountry(arr,25l)))
    assert("England".equals(NetworkUtils.getCountry(arr,28l)))
    assert("England".equals(NetworkUtils.getCountry(arr,30l)))
    assert("Sweden".equals(NetworkUtils.getCountry(arr,31l)))
    assert("Sweden".equals(NetworkUtils.getCountry(arr,37l)))
    assert("Sweden".equals(NetworkUtils.getCountry(arr,45l)))
    assert(null == NetworkUtils.getCountry(arr,100l))

  }
}
