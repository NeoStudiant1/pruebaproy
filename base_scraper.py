# -*- coding: utf-8 -*-

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class DocumentoResultado:
    titulo: str = ""
    autor: str = ""
    anio: str = ""
    idioma: str = ""
    tipo_documento: str = ""
    url_fuente: str = ""
    urls_descarga: List[str] = field(default_factory=list)
    recid: str = ""
    metadatos_extra: dict = field(default_factory=dict)


@dataclass
class FiltrosBusqueda:
    palabras_clave: List[str] = field(default_factory=list)
    anio_desde: Optional[int] = None
    anio_hasta: Optional[int] = None
    idioma: Optional[List[str]] = None
    tipo_documento: Optional[str] = None
    limite: int = 50


class BaseScraper(ABC):

    @abstractmethod
    def nombre_fuente(self) -> str:
        pass

    @abstractmethod
    def search(self, filtros: FiltrosBusqueda) -> List[DocumentoResultado]:
        pass

    @abstractmethod
    def download(self, documento: DocumentoResultado, carpeta_destino: str,
                 intentos_max: int = 3) -> Optional[str]:
        pass
