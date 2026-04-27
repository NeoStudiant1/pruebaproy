# -*- coding: utf-8 -*-
"""
Interfaz comun de los scrapers.

Define las estructuras de datos compartidas y la clase abstracta
BaseScraper de la que cuelgan las implementaciones concretas (una por
fuente). El resto del programa habla con cualquier scraper a traves de
esta interfaz, sin conocer detalles de cada fuente.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Set
import logging

logger = logging.getLogger(__name__)


@dataclass
class DocumentoResultado:
    """Datos de un documento devuelto por una busqueda.

    El campo recid es el identificador interno asignado por la fuente. \
    metadatos_extra queda disponible para que cada scraper agregue 
    informacion propia sin tener que ampliar la dataclass."""
    titulo: str = ""
    autor: str = ""
    fecha: str = ""
    idioma: str = ""
    tipo_documento: str = ""
    url_fuente: str = ""
    urls_descarga: List[str] = field(default_factory=list)
    recid: str = ""
    metadatos_extra: dict = field(default_factory=dict)


@dataclass
class FiltrosBusqueda:
    """Criterios introducidos por el usuario en el menu interactivo.

    El campo idioma se modela como lista para permitir consultas en
    varios idiomas en una misma busqueda; el resto son escalares con
    None como valor neutro (sin filtro)."""
    palabras_clave: List[str] = field(default_factory=list)
    fecha_desde: Optional[int] = None
    fecha_hasta: Optional[int] = None
    idioma: Optional[List[str]] = None
    tipo_documento: Optional[str] = None
    limite: int = 50


class BaseScraper(ABC):
    """Contrato que toda fuente de documentos debe respetar.

    main.py depende unicamente de los tres metodos abstractos definidos
    aqui. Para incorporar una nueva biblioteca digital basta heredar de
    esta clase, implementar nombre_fuente, search y download, y
    registrarla en obtener_scrapers_disponibles() de main.py."""

    @abstractmethod
    def nombre_fuente(self) -> str:
        """Nombre legible de la fuente, usado en menus y logs."""
        pass

    @abstractmethod
    def search(self, filtros: FiltrosBusqueda,
               ids_excluir: Optional[Set[str]] = None) -> List[DocumentoResultado]:
        """Devuelve una lista de DocumentoResultado segun los filtros.

        El parametro ids_excluir es un conjunto opcional de identificadores
        prefijados con fuente ('UN:...', 'ILO:...') que deben omitirse
        silenciosamente; el scraper sigue paginando hasta cubrir
        filtros.limite con documentos no excluidos, o hasta agotar
        resultados. Si es None, ningun documento se omite (este es el
        comportamiento original del proyecto antes de incorporar la
        deteccion de duplicados)."""
        pass

    @abstractmethod
    def download(self, documento: DocumentoResultado, carpeta_destino: str,
                 intentos_max: int = 3) -> Optional[str]:
        """Descarga el PDF asociado al documento.

        Itera sobre documento.urls_descarga, reintenta hasta intentos_max
        veces por URL ante fallos transitorios y guarda el archivo en
        carpeta_destino. Devuelve la ruta del archivo descargado o None
        si todas las URLs fallaron."""
        pass
