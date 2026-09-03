# Protocolo de comunicación

## Formato de mensajes

La comunicación entre cliente y servidor se realiza sobre un socket TCP mediante un protocolo donde cada mensaje se compone de dos partes concatenadas en un único buffer:

- La longitud del mensaje en bytes en formato big-endian
- El contenido del mensaje efectivamente

De esta manera, el socket cuando recibe un mensaje puede saber cuantos bytes tiene que leer para recibir el mensaje completo. Y asi, asegurar la integridad del mismo.

## Manejo de short read / short write

Para evitar mensajes truncados o corruptos, se implementaron dos funciones que garantizan la transferencia completa:

- **`send_all` / `SendAll`**: Envía un buffer reintentando iterativamente mientras queden bytes pendientes hasta completar la transferencia o detectar un error (conexión cerrada, error de socket).
- **`recv_all` / `RecvAll`**: Recibe exactamente la cantidad de bytes requerida, acumulando lecturas sucesivas del socket hasta completar el tamaño exacto esperado o detectar el cierre de la conexión (EOF) antes de tiempo.

Sobre estas primitivas se construyen `send_message`/`SendMessage` y `recv_message`/`RecvMessage`, encargadas de armar y desarmar el encabezado de 4 bytes antes de operar con el payload.

## Tipos de mensajes

El protocolo define tipos de mensaje explícitos que se envian en el primer byte del payload. Toda la lógica de serialización y deserialización se encuentra aislada en el módulo `protocol`:

- `1` (BET): Envío de apuestas desde el **cliente** al **servidor**. Contiene uno o más registros separados por salto de línea (`\n`), codificados como `agency_id,first_name,last_name,document,birthdate,number`.
- `2` (DONE): El **cliente** notifica al **servidor** que finalizó el envío de apuestas para esa agencia solicitando el resultado del sorteo.
- `3` (ACK): El **servidor** le confirma al **cliente** la recepción y almacenamiento correcto de un lote de apuestas.
- `4` (WINNERS): El **servidor** retorna el listado de ganadores pertenecientes a la agencia manteniendo el mismo formato en el cual recibio las mismas (`first_name,last_name,document,birthdate,number` separados por `\n`).
- `5` (BATCH_ERROR): El **servidor** reporta un fallo de validación o procesamiento en el lote enviado.

## Flujo de la comunicación

1. **Envío en lotes:** El cliente procesa el archivo de entrada e incrementa un buffer interno hasta acumular la cantidad de apuestas configurada en `BATCH_SIZE` y luego envía el paquete `BET` con las $N$ apuestas. 
2. **Confirmación:** Espera de manera bloqueante el mensaje `ACK` o `BATCH_ERROR` proveniente del servidor antes de continuar procesando el archivo.
3. **Repetición:** Esto se repite hasta que el cliente haya procesado y enviado todas las apuestas en el INPUT_FILE. 
4. **Cierre y Sorteo:** Una vez procesado todo el archivo, el cliente envía `DONE` y se bloquea esperando la llegada del paquete `WINNERS` para esa agencia, el cual escribe en el archivo `OUTPUT_FILE`.

# Concurrencia y Sincronización

El servidor implementa un modelo en el cual existirá un hilo por cada cliente que se conecte al servidor y así poder atender a más de uno a la vez. El hilo principal se encarga de quedarse esperando por conexiones, aceptarlas y crearles su hilo correspondiente.

## Mecanismos de Sincronización

### **Locks** 
Se utilizan 3 locks independientes:

1. `threads_lock`: Se utiliza cuando el hilo principal crea e inserta un thread en la lista de hilos activos, y cuando el proceso de apagado requiere iterar sobre ellos para joinearlos. Es necesario porque como las listas de Python no son *thread-safe*; si el apagado lee la lista mientras el hilo principal agrega una nueva conexión, se generaría un error de modificación concurrente.

2. `storage_lock`: Se utiliza para *escribir* las apuestas recibidas, *leerlas* y *calcular* los ganadores. Es necesario porque, por ejemplo, si dos agencias envían un lote al mismo tiempo, sin este lock ambos hilos escribirían el archivo simultáneamente, entrelazando las filas y corrompiendolo. Es decir, evita race conditions. 

3. `client_sockets_lock`: Se usa cuando se conecta o desconecta un cliente para actualizar el conjunto de sockets activos. Sirve para que cuando se quiere apagar un hilo, el servidor pueda obtener una captura limpia de los sockets abiertos para forzar su cierre sin interferir con conexiones en curso.

### **Quórum** 
   
Para garantizar que el sorteo se realice únicamente cuando haya finalizado la recepción de un número mínimo de agencias, se utiliza una condvar:

- Cuando una agencia envía el mensaje `DONE`, agrega su `agency_id` al conjunto protegido `finished_agencies`.
- Si la cantidad de agencias finalizadas alcanza el mínimo, el hilo despierta a todos los hilos en espera.
- Si no se alcanza la cuota, el hilo ingresa en espera.

## Graceful Shutdown 

Tanto el cliente como el servidor gestionan la señal `SIGTERM` para liberar recursos de forma limpia: 

- **Servidor:** Al recibir `SIGTERM`, activa una bandera de cierre, cierra el socket de escucha y notifica a la variable de condición del quórum para despertar a los hilos en espera. Luego, otorga un tiempo límite para esperar la finalización de los hilos, forzando el cierre de sockets activos sólo si algún hilo no responde a tiempo. 
- **Cliente:** Utiliza el context de Go para interrumpir el procesamiento del archivo al recibir la señal, cerrando los archivos y sockets abiertos sin enviar mensajes inconsistentes al servidor. 
