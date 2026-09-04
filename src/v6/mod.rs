//! The immutable single-file format.
//!
//! One file holds a data region of compressed blocks followed by one footer.
//! There are no overlay layers, no sidecars and no tombstones. A file is built
//! once by a writer and then only read.
//!
//! This module is crate-private while the delta modules still exist. It takes
//! their place at the top level once the writer and reader are complete.

// Nothing calls into this module until the writer and reader land.
#![allow(dead_code, unused_imports)]

pub(crate) mod attr;
pub(crate) mod footer;

pub(crate) use attr::{AttributeValue, DiskValue, StringPool};
pub(crate) use footer::{ArrayMeta, FOOTER_VERSION, Footer, read_footer};
