# Protocolo de comunicación

## Formato de mensajes

La comunicación entre cliente y servidor se realiza sobre un socket TCP mediante un protocolo donde cada mensaje se compone de dos partes concatenadas en un único buffer:

- La longitud del mensaje en bytes en formato big-endian
- El contenido del mensaje efectivamente

De esta manera, el socket cuando recibe un mensaje puede saber cuantos bytes tiene que leer para recibir el mensaje completo. Y asi, asegurar la integridad del mismo. 

## Manejo de short read / short write

Para evitar mensajes truncados o corruptos, se implementaron dos funciones de bajo nivel que garantizan la transferencia completa:

- **`send_all` / `SendAll`**: envía el buffer completo, reintentando en un loop mientras queden bytes pendientes, hasta que se hayan escrito todos o se detecte un error (conexión cerrada, error de socket).
- **`recv_all` / `RecvAll`**: recibe exactamente `size` bytes, acumulando en un loop los fragmentos recibidos en sucesivas llamadas, hasta completar el tamaño esperado o detectar el cierre de la conexión (`EOF`) antes de tiempo.

Sobre estas dos primitivas se construyen `send_message`/`SendMessage` y `recv_message`/`RecvMessage`, que arman y desarman el framing de longitud + payload descripto arriba.

## Tipos de mensajes

Se definieron **tipos de mensaje explícitos**, identificados por un byte al inicio del payload. Esta parte, esta implementada en un modulo separado (`protocol`). Este modulo solo sabe transformar datos crudos en bytes y viceversa; no abre sockets ni conoce la lógica de sorteo.

- `1` (BET): El **cliente** envía una apuesta individual al **servidor** (`agency_id,first_name,last_name,document,birthdate,number`).
- `2` (DONE): El **cliente** indica que la agencia terminó de enviar apuestas y le solicita el resultado del sorteo al **servidor**. 
- `3` (ACK): El **servidor** le confirma que una apuesta fue almacenada al **cliente**.
- `4` (WINNERS): El **servidor** devuelve el listado de ganadores de la agencia al **cliente** (uno o más registros separados por salto de línea, cada uno `first_name,last_name,document,birthdate,number`).

## Flujo de la comunicación

Por cada línea del archivo de entrada, el cliente arma un mensaje `BET`, lo envía y bloquea esperando el `ACK` correspondiente antes de continuar con la siguiente línea. Al agotar el archivo, envía un mensaje `DONE` y espera la respuesta `WINNERS`, que persiste en el `OUTPUT_FILE`.