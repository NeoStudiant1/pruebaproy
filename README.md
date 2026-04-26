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
   - Rango de fechas: Año desde y año hasta (opcional).
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
  Anio desde (ej: 2015): 2020
  Anio hasta (ej: 2024): 2024
  Codigo de idioma (ej: es): en
  Tipo de documento: reporte
  Numero maximo de documentos (default: 50): 10
```


## Archivos generados

| Archivo | Descripcion |
|---------|-------------|
| `documentos_descargados/*.pdf` | Los PDFs descargados |
| `documentos_descargados/metadata.csv` | Metadatos en formato CSV |
| `errores.log` | Log detallado de la sesion (para depuracion) |

### Formato del CSV de metadatos

El archivo `metadata.csv` contiene las columnas:
- titulo
- autor
- anio
- idioma
- tipo_documento
- url_fuente
- archivo_local (nombre del PDF o "DESCARGA_FALLIDA")

Se puede abrir con Excel, Google Sheets o cualquier editor de texto.


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
