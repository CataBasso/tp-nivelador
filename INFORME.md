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
