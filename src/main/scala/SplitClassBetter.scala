package con.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object SplitClassBetter {

  var cad = "YH06123456UX09JuanLopezEM19juanlopez@gmail.comFG02JL"

  //YH06123456UX09JuanLopezEM19juanlopez@gmail.comFG02JL

  def Itera(cadena:String): Unit = {
    var indice = 0
    var longitud = 0
    var array = Array("UX", "JR", "FG", "YH", "PR")
    var tagfind = true
    var valor = " "

    while (cad.nonEmpty) {
      //1. Recortar el valor de tag
      var tag = cad.substring(indice, indice + 2)
      //2.Buscar tag dentro de array
      tagfind = array.contains(tag)
      //println(tagfind)
      //3. Si es true entonces calcular la longitud
      //Poner if de condicion si es true
        longitud = cad.substring(indice + 2, indice + 4).toInt
        //4. Recortar la cadena por el valor de la longitud - indice + 4
        valor = cad.substring(indice + 4, indice + 4 + longitud) //VALOR = 123456
      //Solo vamos a imprimir los true
        if (tagfind == true) {
          //println(valor + "\n")
          //Podriamos concatenar el valor de tag mas su valor
          println(tag + " " + valor)
        }
        //Repetir mientras cadena no este vacia --> while (cad.nonEmpty)
        //5. Hacer indice empiece en el siguiente elemento de la cadena
        indice = indice + 4 + longitud // indice = 4 + 6 = 10
        //Recortar cadena y quedarse con los siguientes elementos
        cad = cad.substring(indice, cad.length()) // UX09JuanLopezEM19juanlopez@gmail.comFG02JL
        //Empieza de nuevo en 0 para evitar errores en array
        indice = 0 //Empieza de nuevo en U de UX09JuanLopezEM19juanlopez@gmail.comFG02JL
    }
  }


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create a SparkContext using every core of the local machine
    val sc = new SparkContext(master = "local[*]", appName = "SplitClass")

    Itera(cad)
    }
} //main

//1. Recortar los primeros 4 elementos de la cadena
//2. Si los primeros 2 coinciden con el array entonces:
//3. Recortar los 2 caracteres siguientes y asignarlos a la variable longitud
//4. Recortar los siguientes elementos con base en la variable longitud
//5. Repetir

// while y luego for

/*"YH06123456UX09JuanLopezEM19juanlopez@gmail.comFG02JL"


tag = YH

var array = Array("UX", "JR", "FG", "YH")

YH = UX ? No --> Sale del ciclo

YH != JR

YH != FG

YH == YH

------

UX == UX

----

EM != UX, JR, FG, YH

Vuelve a tomar EM

--> Eliminar EM + longitud + caracteres de longitud

--> Debemos guardar el valor de cadena del if


var tag = cad.substring(indice, indice + 2)//YH
longitud = cad.substring(indice + 2, indice + 4).toInt // longitud = YH06123456UX09JuanLopezEM19juanlopez@gmail.comFG02JL.substring(0 + 2, 0 + 4) = 06 = 6
//valor a extraer
var valor = cad.substring(indice + 4, indice + 4 + longitud) //VALOR = 123456
println(valor + "\n")
indice = indice + 4 + longitud // indice = 4 + 6 = 10
cadena = cad.substring(indice, cad.length()) // UX09JuanLopezEM19juanlopez@gmail.comFG02JL
indice = 0 //Empieza de nuevo en U de UX09JuanLopezEM19juanlopez@gmail.comFG02JL

*/

