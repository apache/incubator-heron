'''module for base component'''
__all__ = ['base_component', 'component_spec']

from .base_component import BaseComponent, NotCompatibleError
from .component_spec import HeronComponentSpec, GlobalStreamId
