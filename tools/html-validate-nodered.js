'use strict';

// html-validate transformer for Node-RED node HTML files.
//
// A Node-RED node's .html file is not a document — it is a container of
// <script> blocks: one `text/javascript` block that registers the node with the
// editor, plus `text/html` blocks holding the edit dialog template and the help
// panel. Essentially all of the markup lives inside those `text/html` blocks.
//
// That matters because an HTML validator treats script content as opaque text,
// so pointing one at `nodes/*.html` directly validates only the handful of
// <script> tags themselves and reports success while looking at nothing. This
// transformer hands html-validate each `text/html` block as its own source, with
// the line and column of the block's start, so reported positions point at the
// real file rather than at an extracted copy.
//
// `text/javascript` blocks are deliberately not yielded: they are JavaScript, and
// an HTML validator has nothing useful to say about them.
const BLOCK = /<script\s+type="text\/html"[^>]*>([\s\S]*?)<\/script>/g;

function transformer(source) {
  const sources = [];
  for (const match of source.data.matchAll(BLOCK)) {
    const openTag = match[0].slice(0, match[0].indexOf('>') + 1);
    const contentOffset = match.index + openTag.length;
    const before = source.data.slice(0, contentOffset);
    const lines = before.split('\n');
    sources.push({
      data: match[1],
      filename: source.filename,
      // The content starts immediately after the open tag, which is followed by a
      // newline, so the block's first markup line is the next one.
      line: lines.length,
      column: lines[lines.length - 1].length + 1,
      offset: contentOffset,
      originalData: source.data,
    });
  }
  return sources;
}

transformer.api = 1;

module.exports = transformer;
