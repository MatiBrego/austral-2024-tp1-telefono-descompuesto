Debe implementar los componentes del sistema del teléfono descompuesto como continuación del trabajo práctico anterior.

La implementación de referencia que se va a usar para la evaluación está acá: [https://github.com/marianobntz/austral-2024-tp1-telefono-descompuesto/tree/tp2](https://github.com/marianobntz/austral-2024-tp1-telefono-descompuesto/tree/tp2 "https://github.com/marianobntz/austral-2024-tp1-telefono-descompuesto/tree/tp2")   

La arquitectura es la siguiente:

1.  El juego se forma con un _coordinador_ (nodo principal) y varios nodos _participantes_.
    1.  Los _participantes_ pueden entrar y salir del juego en cualquier momento.
2.  El objetivo del juego es pasar un mensaje enviado al _coordinador_ por todos los _participantes_ que deben "firmar" el mensaje y pasarlo de vuelta al _coordinador_ cuando TODOS los _participantes_ hayan firmado. 
3.  Cada _jugada_ es el envío de un mensaje y su verificación.
4.  El coordinador recibe por configuración el tiempo máximo en segundos para esperar a todos los participantes para firmar un mensaje (en una jugada), también genera una clave privada para usar cuando valida mensajes.
5.  Los _participantes_ reciben por configuración la dirección (host y port) del _coordinador_ y generan un UUID único que los identifica y una clave privada para firmar los mensajes. 
6.  Cuando un nuevo _participante_ quiere participar del juego se registra con el _coordinador_ (POST a /register-node). 
    1.  Como parámetros en el body envía:
    1.  Su dirección (host y port),
    2.  su UUID,
    3.  su clave privada.
2.  Como respuesta el _coordinador_ le devuelve en el body de la respuesta:
    1.  la dirección (host y port) para reenviar los mensajes que reciba (el siguiente participante del juego),

    2.  el tiempo máximo en segundos para declarar como fallada una jugada (desde que se envía del primer al último nodo).

    3.  El X-Game-Timestamp (ver mas abajo) que indica a partir de cuando va a recibir jugadas el nuevo _participante_.
3.  A partir de la registración el participante puede recibir jugadas de parte del coordinar o de otro participante.
4.  El coordinador devuelve un status diferente según los parámetros recibidos
    1.  200 si la registración fue correcta y se agrega un **nuevo** participante.
    2.  202 si el uuid y clave ya están registrados, se devuelven los mismos datos que se habían pasado anteriormente
    3.  400 si alguno de los parámetros es inválido (vacío por ejemplo)
    4.  401 Si el uuid ya existe pero la clave privada es distinta a la registrada previamente.
7.  Cada jugada del juego consiste en un mensaje (POST a /play) enviado al _coordinador_ y este devuelve el resultado de la jugada en forma sincrónica. 
    1.  Este resultado incluye si el sistema funcionó o tuvo problemas (éxito o falla).
2.  El formato de la respuesta en cualquier caso es un JSON con el SHA generado por el coordinador y las firmas de todos los participantes. 
    3.  Devuelve 200 si el mensaje llegó correctamente y están todas las firmas de los _participantes_ que estaban al **inicio** de la jugada.
4.  Devuelve 400 si el juego está cerrado y no se aceptan jugadas (indica que el sistema falló, ver más abajo)

5.  Devuelve 500 si el mensaje volvió correctamente (el mismo contenido) pero faltan firmas
6.  Devuelve 503 si el mensaje no volvió correctamente, no importan las firmas. (esto se calcula con el SHA del contenido de la parte "message")

7.  Devuelve 504 si los _participantes_ no completan la vuelta a tiempo (no llega el último POST a /relay al coordinador antes del timeout definido)
8.  El coordinador envía el mensaje al último _participante_ agregado a través de un POST a /relay para que circulen el mensaje entre todos los _participantes_.
    1.  Los datos son (ver más abajo el formato multipart):
    2.  El mensaje original como una parte llamada "message"
    3.  Las firmas se envían dentro de una parte llamada "signatures" que es un JSON con un array de firmas (UUID del participante y el hash calculado).
    4.  Se envía un dato adicional llamado "X-Game-Timestamp" como Header de HTTP que es un número entero que se incrementa con cada jugada. Ese dato debe viajar entre los _participantes_ y volver al _coordinador._
9.  Los _participantes_ se limitan a firmar y reenviar los mensajes que reciban (reciben un POST a /relay y hacen un POST a /relay al siguiente _participante_)
    1.  La "firma" de un mensaje por un _participante_ consiste en calcular un hash con SHA-256 sobre el contenido del mensaje solamente (agregando la clave privada interna como "salt"). 
    2.   (la implementación de referencia tiene el código para calcular el hash).
    3.  El participante debe invocar al /relay en el nodo que tiene configurado con los mismos parámetros (partes y headers) que recibió, salvo el agregado de su firma a la parte de **signatures**.
    4.  El participante **debe** descartar llamadas que tengan un X-Game-Timestamp menor o igual al valor del último mensaje procesado correctamente.
    5.  Todos los _participantes_ tiene la opción de enviar el mismo POST a /relay al nodo _coordinador_ ante cualquier problema. Si el _coordinador_ recibe el mensaje sin todas las firmas va a declarar fallida la jugada (si faltan firmas).
    6.  Como respuesta al POST /relay debe devolver:
        1.  200 si el mensaje fue firmado y enviado correctamente al siguiente _participante_.
        2.  202 si el mensaje fue aceptado pero sin confirmación de que se envió al siguiente participante.
        3.  400 si el X-Game-Timestamp del parámetro es inválido.
        4.  503 si no se pudo enviar el POST a /relay al siguiente _participante (el siguiente nodo)_.
10.  Cuando un _participante_ decide abandonar el juego **debe** avisar al coordinador mediante un (POST a /unregister-node) indicando su identificador único y la clave que registró.  
     1.  El _participante_ **puede** esperar un tiempo desde que aviso al _coordinador_ para participar en jugadas que hayan comenzado antes de avisar.
     2.  El coordinador va a devolver los siguientes status:
    1.  202 si el unregister está aceptado
    2.  400 si los datos no son válidos
11.  El _coordinador_ **puede** en cualquier momento cambiar el registro de un _participante mediante un POST a /reconfigure que incluye:_
     1.  El UUID del participante
     2.  la clave privada que envió originalmente
     3.  la nueva dirección (host y port) del siguiente nodo.
     4.  El header X-Game-Timestamp a partir del cual van a llegar jugadas con la nueva configuración.
     5.  El _participante_ **debe** validar que el UUID y la clave sean las que tiene internamente.
     6.  El participante **puede** elegir el criterio para hacer el cambio de dirección: puede esperar un tiempo o a que llegue el timestamp informado.
     7.  El POST a /reconfigure devuelve 200 si el cambio se aceptó o 400 si no son validos los datos.
12.  El objetivo final del juego es que se pueda jugar continuamente mientras entran y salen participantes.

1.  **El juego se va a cortar (el coordinador no acepta más jugadas) si ocurren más de 10 fallas por timeout (respuesta con status 503 o 504 en el POST a /play)**
2.  **_El objetivo de los participantes es evitar que el juego se corte._**

14.  El protocolo dejó ciertos puntos a discreción del implementador pero que no alteran el protocolo. 
15.  _**Su misión es lograr una implementación que evite que el juego se corte en la mayor cantidad de situaciones posibles.**_



*   El contrato está definido en el archivo de openapi llamado openapi.json. Ese es el contrato de los métodos que componen el sistema y deben respetar para la interoperabilidad.
*   La estructura del trabajo práctico se basa en usar el concepto de **multipart/form-data**
*   La implementación base usa java 17, kotlin y gradle 8.10. Se puede implementar el código _participante_ en cualquier forma mientras cumpla con el contrato de openapi.



### Implementación

Esa implementación está parcialmente desarrollada. Deben implementar las funciones del _coordinador_ y del _participante_.   

Debe ser un solo runtime que pueda funcionar como _coordinador_ o _participante_ según los parámetros de configuración. 

El runtime debe funcionar por lo menos contra si mismo (el mismo runtime corriendo una instancia de coordinador y 3 de participante). 

Las entregas son individuales: me envían el código fuente del proyecto con el build de compilación. Pueden (deben) usar el proyecto que indiqué para compilar.



Las pruebas se van a hacer en clase probando el sistema entre todos los alumnos que hayan entregado. 

Saludos.