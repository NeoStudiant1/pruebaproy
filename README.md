# Descargador de Documentos - Bibliotecas Digitales

Herramienta de linea de comandos para buscar y descargar documentos PDF
desde la Biblioteca Digital de Naciones Unidas (UN Digital Library) y el
repositorio Labordoc de la Organizacion Internacional del Trabajo (OIT).

## Requisitos previos

- Python 3.9 o superior
- Conexion a internet
- Windows 10/11, macOS o Linux


## Instrucciones de instalacion (paso a paso para VS Code)

Estas instrucciones estan escritas para personas sin experiencia previa
en programacion. Si ya tienes Python y VS Code configurados, puedes
saltar directamente al paso 3.

### Paso 1: Instalar Python

**Windows:**
1. Ve a https://www.python.org/downloads/
2. Descarga la version mas reciente (boton amarillo grande).
3. Ejecuta el instalador.
4. IMPORTANTE: Marca la casilla "Add Python to PATH" antes de hacer clic
   en "Install Now".
5. Espera a que termine la instalacion y cierra el instalador.

**macOS:**
1. Abre la Terminal (busca "Terminal" en Spotlight).
2. Escribe: `brew install python3`
   Si no tienes Homebrew, instala Python desde https://www.python.org/downloads/

**Linux (Ubuntu/Debian):**
1. Abre una terminal.
2. Escribe: `sudo apt update && sudo apt install python3 python3-pip`

### Paso 2: Instalar Visual Studio Code

1. Ve a https://code.visualstudio.com/
2. Descarga la version para tu sistema operativo.
3. Instala VS Code con las opciones por defecto.
4. Abre VS Code.
5. (Opcional) Instala la extension "Python" de Microsoft:
   - Haz clic en el icono de extensiones (barra lateral izquierda).
   - Busca "Python" y haz clic en "Install" en la primera opcion.

### Paso 3: Abrir el proyecto en VS Code

1. Descarga o copia la carpeta `proyecto_scraper` a tu computadora.
2. En VS Code, ve a Archivo > Abrir carpeta (File > Open Folder).
3. Selecciona la carpeta `proyecto_scraper`.
4. VS Code abrira el proyecto y veras los archivos en el panel izquierdo.

### Paso 4: Abrir la terminal integrada

1. En VS Code, ve a Terminal > Nueva terminal (Terminal > New Terminal).
   Tambien puedes usar el atajo: Ctrl + ` (la tecla al lado del 1).
2. Se abrira una terminal en la parte inferior de la pantalla.
3. Verifica que la terminal este en la carpeta del proyecto.
   Deberia mostrar algo como: `C:\Users\TuNombre\proyecto_scraper>`

### Paso 5: Instalar dependencias

Escribe los siguientes comandos en la terminal de VS Code, uno por uno,
presionando Enter despues de cada uno:

```
pip install -r requirements.txt
```

Esto instalara: requests, playwright y lxml.

Luego, instala el navegador que Playwright necesita:

```
playwright install chromium
```

Esto descargara el navegador Chromium (unos 150 MB). Es necesario
para acceder a Labordoc, que requiere JavaScript para funcionar.

### Paso 6: Verificar la instalacion

Escribe en la terminal:

```
python main.py
```

Selecciona la opcion 2 (Diagnostico de dependencias) para verificar
que todo este correctamente instalado.

**Si aparece "python no se reconoce como comando":**
- En Windows, intenta con `py main.py` en lugar de `python main.py`.
- Si sigue sin funcionar, reinstala Python asegurandote de marcar
  "Add Python to PATH".


## Como usar el programa

### Ejecucion

Desde la terminal de VS Code:

```
python main.py
```

### Flujo del menu

1. **Menu principal:** Selecciona "Buscar y descargar documentos".

2. **Seleccionar fuente:** Elige entre UN Digital Library o ILO Labordoc.

3. **Configurar filtros:**
   - Palabras clave (obligatorio): Escribe los terminos separados por coma.
     Ejemplo: `climate change, sustainable development`
   - Rango de fechas: Fecha desde y fecha hasta (opcional).
   - Idioma: Codigo de dos letras (en, es, fr, ar, zh, ru) o dejar vacio.
   - Tipo de documento: reporte, resolucion, acuerdo, decision, carta.
   - Limite de documentos: Por defecto 50. Maximo recomendado 200.

4. **Confirmar:** Revisa el resumen y confirma con "s".

5. **Resultados:**
   - Los PDFs se guardan en `./documentos_descargados/` (o la carpeta elegida).
   - Se genera un archivo `metadata.csv` con los datos de cada documento.
   - Se muestra un resumen de documentos exitosos y fallidos.

### Ejemplo de sesion

```
  Palabras clave (separadas por coma): child labour, forced labour
  Fecha desde (ej: 2015): 2020
  Fecha hasta (ej: 2024): 2024
  Codigo de idioma (ej: es): en
  Tipo de documento: reporte
  Numero maximo de documentos (default: 50): 10
```


## Archivos generados

| Archivo | Descripcion |
|---------|-------------|
| `documentos_descargados/*.pdf` | Los PDFs descargados |
| `documentos_descargados/metadata.csv` | Metadatos en formato CSV (tabular, para Excel/Sheets) |
| `documentos_descargados/metadata.json` | Metadatos en formato JSON (estructurado, con texto completo) |
| `documentos_descargados/textos_extraidos.txt` | Texto consolidado de todos los PDFs descargados |
| `historial_descargas.json` | Indice acumulado de todos los documentos ya descargados (en la raiz del proyecto) |
| `errores.log` | Log detallado de la sesion (para depuracion) |

### Formato de los metadatos (CSV y JSON)

Ambos archivos contienen los mismos campos para cada documento:
- `titulo`
- `autor`
- `fecha` — fecha de publicacion del documento
- `idioma`
- `tipo_documento`
- `url_fuente`
- `archivo_local` — nombre del PDF o "DESCARGA_FALLIDA"
- `fecha_descarga` — momento exacto de la descarga, formato ISO 8601 (ej: `2026-04-21T15:42:31`)
- `texto_extraido` — contenido textual del PDF

**Diferencia importante entre CSV y JSON:**
- En el `metadata.csv`, el campo `texto_extraido` se TRUNCA a los primeros 500
  caracteres seguido de un indicador, para que el archivo se pueda abrir comodamente
  en Excel sin que las celdas se vuelvan ilegibles.
- En el `metadata.json`, el campo `texto_extraido` contiene el texto COMPLETO sin
  truncar. Es la fuente canonica del contenido textual.

El `metadata.csv` se genera con pandas. El `metadata.json` es mas conveniente
para procesar los datos desde otros programas o scripts.

### Archivo de textos consolidado (`textos_extraidos.txt`)

Contiene el texto extraido de todos los PDFs en un solo archivo, separados
por encabezados claros que indican el archivo, titulo, fecha de publicacion
y fecha de descarga de cada documento:

```
======================================================================
DOCUMENTO 1 de 15
ARCHIVO:           ILO_alma995339593202676_Issue_paper.pdf
TITULO:            Issue paper on child labour and climate change
FECHA PUBLICACION: 2023
FECHA DESCARGA:    2026-04-21T15:42:31
======================================================================

[texto del PDF aqui]

```

### PDFs sin texto extraible (escaneados)

Algunos PDFs son escaneos de documentos en papel: contienen imagenes en lugar
de texto digital. Estos no se pueden leer con extraccion simple.

Cuando el programa detecta un PDF asi, el campo `texto_extraido` muestra:
`[PDF SIN CAPA DE TEXTO - OCR REQUERIDO]`

El PDF queda descargado correctamente, simplemente no se le puede sacar texto
sin un proceso adicional de OCR (reconocimiento optico de caracteres). Esto
podria agregarse en una version futura del programa si fuera necesario.

Otros marcadores que pueden aparecer en `texto_extraido`:
- `[PDF VACIO O SIN CONTENIDO TEXTUAL]` — el PDF no tenia paginas con texto
- `[ERROR AL LEER EL PDF]` — el archivo estaba corrupto o protegido
- `[ARCHIVO NO DESCARGADO]` — la descarga del PDF fallo


## Historial acumulado de descargas

El archivo `historial_descargas.json` (en la raiz del proyecto) guarda un
indice de todos los documentos que el programa ha intentado descargar, sean
exitosos o fallidos. Esto evita que:

- Descargues dos veces el mismo documento aunque corras busquedas repetidas.
- Reintentes documentos que ya fallaron anteriormente (algunos documentos
  simplemente no tienen PDF disponible y seguirian fallando).

### Como funciona

Al iniciar una busqueda, el programa lee el historial y le pasa al scraper
la lista de documentos ya conocidos. El scraper los salta silenciosamente
y sigue buscando hasta completar el numero de documentos que pediste.

Ejemplo: si pides 15 documentos sobre "climate change" y el programa detecta
que 12 ya estan en tu historial, buscara en mas paginas hasta conseguir 15
documentos nuevos reales (o hasta que no queden mas en la fuente).

Al final de la sesion se muestra cuantos documentos hay en total en el
historial acumulado.

### Como borrar o editar el historial

- Para **borrar todo el historial** (empezar de cero): borra el archivo
  `historial_descargas.json`. El programa lo crea de nuevo la siguiente
  sesion.
- Para **borrar entradas especificas** (forzar re-descarga de ciertos
  documentos): abre el archivo con el Bloc de notas, busca la entrada
  correspondiente por su ID o titulo, y borra ese bloque. Cuida de no
  romper la estructura JSON (comas, llaves).
- Para **inspeccionar que tienes descargado**: el archivo es legible en
  cualquier editor de texto. Cada entrada incluye titulo, fuente, fecha
  de publicacion, fecha de descarga, ruta del archivo y estado (exitoso
  o fallido).

Si el archivo se corrompe por error, el programa muestra un mensaje claro
al iniciar y sigue funcionando sin la deteccion de duplicados hasta que lo
arregles o lo borres.


## Solucion de problemas

### "No se encontraron documentos"
- Verifica que las palabras clave sean correctas.
- Amplia el rango de fechas o elimina filtros restrictivos.
- Prueba primero sin filtros de idioma o tipo de documento.

### "Error de conexion" o "Timeout"
- Verifica tu conexion a internet.
- Los servidores pueden estar temporalmente lentos. Espera unos minutos.
- Si usas VPN, desactivala temporalmente.

### "Playwright no esta instalado"
- Ejecuta: `pip install playwright`
- Luego: `playwright install chromium`

### "Descarga fallida" en muchos documentos
- Algunos documentos no tienen PDF disponible (solo metadatos).
- Revisa `errores.log` para ver el detalle de cada error.
- Los enlaces pueden requerir acceso institucional.

### Compartir errores para obtener ayuda
Si el programa falla, comparte el archivo `errores.log`. Contiene
marcas de tiempo, URLs que fallaron, tipo de error y la traza completa.
Cada error esta separado con lineas "---" para facilitar la lectura.


## Notas tecnicas

### UN Digital Library
- Usa la API REST publica basada en Invenio (CERN).
- No requiere autenticacion ni navegador headless.
- Formato de datos: MARCXML.
- Limite implicito del servidor en registros por solicitud.

### ILO Labordoc
- Usa Ex Libris Primo VE (SPA Angular).
- Requiere Playwright para renderizar JavaScript.
- Los PDFs pueden estar en dominios variados (ilo.org, repositorios).
- Es mas lenta que la fuente de la ONU debido al renderizado.

### Limites y uso responsable
- El programa incluye pausas entre solicitudes para no sobrecargar
  los servidores.
- Se recomienda no exceder 200 documentos por sesion.
- Estos repositorios son recursos publicos; usarlos con responsabilidad.


## Agregar nuevas fuentes

Consulta las instrucciones detalladas al final de `main.py` en la seccion
"COMO AGREGAR UNA NUEVA FUENTE". En resumen:

1. Crea un archivo `scraper_nueva_fuente.py`
2. Hereda de `BaseScraper` e implementa `nombre_fuente()`, `search()` y `download()`
3. Registra el scraper en `obtener_scrapers_disponibles()` dentro de `main.py`
4. Prueba con una busqueda pequena
5. Documenta la estrategia y dependencias


## Archivo de configuracion (configuracion.json)

El archivo `configuracion.json` se crea automaticamente la primera vez que
ejecutas el programa. Contiene parametros que puedes cambiar sin tocar
codigo Python:

- `fecha_minima_permitida`: fecha mas antigua que el programa acepta (default: 1945).
- `fecha_maxima_permitida`: fecha mas reciente que el programa acepta (default: 2026).
- `carpeta_descarga_por_defecto`: carpeta donde se guardan los PDFs.
- `limite_documentos_por_defecto`: cuantos documentos buscar como maximo.
- `ultima_carpeta_usada`: se actualiza automaticamente con la ultima carpeta elegida.
- `ilo_search_scope`: scope de busqueda de ILO Labordoc (default: ALL_ILO).
- `ilo_tab`: tab de busqueda de ILO Labordoc (default: ALL_ILO).
- `idiomas_validos`: codigos de idioma reconocidos y sus nombres en espanol.

Si el archivo se corrompe (por ejemplo, si se borra una coma o una comilla),
el programa mostrara un mensaje de error claro indicando que linea tiene el
problema, y seguira funcionando con valores por defecto.


## Como ampliar el rango de fechas (por ejemplo, para usar el programa en 2027)

El programa valida que las fechas ingresadas esten dentro de un rango
permitido. Por defecto, la fecha maxima es 2026. Si necesitas buscar
documentos de 2027 en adelante, sigue estos pasos:

1. Abre el archivo `configuracion.json` con el Bloc de notas (Windows)
   o cualquier editor de texto.

2. Busca la linea que dice:
       "fecha_maxima_permitida": 2026,

3. Cambia el numero 2026 por la fecha que necesites, por ejemplo:
       "fecha_maxima_permitida": 2027,

4. Guarda el archivo y cierra el Bloc de notas. La proxima vez que
   ejecutes el programa, aceptara fechas hasta 2027.

IMPORTANTE: no borres las comillas, las comas ni las llaves del archivo.
Si el programa muestra un error al iniciar, revisa que el formato sea
correcto o borra el archivo `configuracion.json` para que se regenere
con los valores por defecto.
