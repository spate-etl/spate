import type {PrismTheme} from 'prism-react-renderer';

/**
 * The code palette on both grounds: a dark block, as the brand direction
 * draws it, with the accent on keywords. Literal hex on purpose, since a
 * Prism theme cannot read a custom property; the values are brand.css's
 * dark-ground code tokens and clear 4.5:1 on the block.
 */
const theme: PrismTheme = {
  plain: {color: '#e8e9ec', backgroundColor: '#111318'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#9aa1a9'}},
    {types: ['keyword', 'operator', 'atrule', 'important', 'lifetime-annotation'], style: {color: '#ff8c4a'}},
    {types: ['function', 'class-name', 'builtin', 'tag', 'selector', 'namespace'], style: {color: '#8fd3ff'}},
    {types: ['string', 'char', 'attr-value', 'inserted', 'regex'], style: {color: '#b8e986'}},
    {types: ['number', 'boolean', 'constant', 'symbol'], style: {color: '#ffc9a8'}},
    {types: ['punctuation'], style: {color: '#b6bcc4'}},
    {types: ['attr-name', 'property', 'variable', 'macro', 'key'], style: {color: '#e8e9ec'}},
    {types: ['deleted'], style: {color: '#ff8a7a'}},
  ],
};

export default theme;
