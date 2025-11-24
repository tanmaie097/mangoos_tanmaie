object helloworld {
    def main(args: Array[String]) = {
        val k = "hello world"
        var count = 0
        val arr = k.split(" ")
        for _ <- arr do
                count += 1
        println(count)
    }
}