# -*- coding: utf-8 -*-
from pygments.style import Style
from pygments.token import Token, Comment, Name, Keyword, Generic, Number, \
    Operator, String

BASE03 = '#002B36'
BASE02 = '#073642'
BASE01 = '#586E75'
BASE00 = '#657B83'
BASE0 = '#839496'
BASE1 = '#93A1A1'
BASE2 = '#EEE8D5'
BASE3 = '#FDF6E3'
YELLOW = '#B58900'
ORANGE = '#CB4B16'
RED = '#DC322F'
MAGENTA = '#D33682'
VIOLET = '#6C71C4'
BLUE = '#268BD2'
CYAN = '#2AA198'
GREEN = '#719e07'


class SolarizedDarkStyle(Style):

    """ Dark version solarized theme (http://ethanschoonover.com/solarized). """
    background_color = BASE03
    styles = {
        Keyword: GREEN,
        Keyword.Constant: ORANGE,
        Keyword.Declaration: BLUE,
        # Keyword.Namespace
        # Keyword.Pseudo
        Keyword.Reserved: BLUE,
        Keyword.Type: RED,

        # Name
        Name.Attribute: BASE1,
        Name.Builtin: YELLOW,
        Name.Builtin.Pseudo: BLUE,
        Name.Class: BLUE,
        Name.Constant: ORANGE,
        Name.Decorator: BLUE,
        Name.Entity: ORANGE,
        Name.Exception: ORANGE,
        Name.Function: BLUE,
        # Name.Label
        # Name.Namespace
        # Name.Other
        Name.Tag: BLUE,
        Name.Variable: BLUE,
        # Name.Variable.Class
        # Name.Variable.Global
        # Name.Variable.Instance

        # Literal
        # Literal.Date
        String: CYAN,
        String.Backtick: BASE01,
        String.Char: CYAN,
        String.Doc: BASE1,
        # String.Double
        String.Escape: ORANGE,
        String.Heredoc: BASE1,
        # String.Interpol
        # String.Other
        String.Regex: RED,
        # String.Single
        # String.Symbol
        Number: CYAN,
        # Number.Float
        # Number.Hex
        # Number.Integer
        # Number.Integer.Long
        # Number.Oct

        Operator: GREEN,
        # Operator.Word

        # Punctuation: ORANGE,

        Comment: BASE01,
        # Comment.Multiline
        Comment.Preproc: GREEN,
        # Comment.Single
        Comment.Special: GREEN,

        # Generic
        Generic.Deleted: RED,
        Generic.Emph: 'italic',
        Generic.Error: RED + ' bold',
        Generic.Heading: ORANGE,
        Generic.Inserted: GREEN,
        # Generic.Output
        # Generic.Prompt
        Generic.Strong: 'bold',
        Generic.Subheading: BLUE,
        # Generic.Traceback

        Token: BASE1,
        Token.Other: ORANGE,
    }
