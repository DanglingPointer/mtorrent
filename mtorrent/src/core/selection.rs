use super::ctx;
use crate::app::main::Mode;
use local_async_utils::shared::Shared;
use std::io;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SelectionError {
    #[error("invalid file index {0}")]
    InvalidFileIndex(usize),
    #[error("cannot exclude files from a single-file torrent")]
    SingleFileTorrent,
    #[error("file exclusion is only supported in leech mode")]
    UnsupportedMode,
}

impl From<SelectionError> for io::Error {
    fn from(e: SelectionError) -> Self {
        let kind = match &e {
            SelectionError::InvalidFileIndex(_) => io::ErrorKind::InvalidInput,
            SelectionError::SingleFileTorrent => io::ErrorKind::InvalidInput,
            SelectionError::UnsupportedMode => io::ErrorKind::Unsupported,
        };
        io::Error::new(kind, e)
    }
}

/// Make sure pieces that lie entirely within excluded files are never downloaded. Pieces that
/// also overlap wanted files are still downloaded. Nothing is skipped if an error is returned.
pub fn exclude_files(
    ctx_handle: &ctx::Handle<ctx::MainCtx>,
    excluded_files: &[usize],
) -> Result<(), SelectionError> {
    ctx_handle.with(|ctx| forget_excluded_pieces(ctx, excluded_files.iter().copied()))
}

fn forget_excluded_pieces(
    ctx: &mut ctx::MainCtx,
    excluded_files: impl IntoIterator<Item = usize>,
) -> Result<(), SelectionError> {
    let mut excluded_files = excluded_files.into_iter().peekable();
    if excluded_files.peek().is_none() {
        return Ok(());
    }

    if !matches!(ctx.const_data.mode(), Mode::Leech) {
        return Err(SelectionError::UnsupportedMode);
    }

    let Some(files) = ctx.metainfo.files() else {
        return Err(SelectionError::SingleFileTorrent);
    };

    struct Position {
        global_offset: usize,
        len: usize,
    }

    let mut next_global_offset = 0;
    let mut files: Vec<_> = files
        .map(|(file_len, _)| {
            let global_offset = next_global_offset;
            next_global_offset += file_len;
            let is_excluded = file_len == 0; // an empty file shouldn't keep a piece from being excluded
            (
                Position {
                    global_offset,
                    len: file_len,
                },
                is_excluded,
            )
        })
        .collect();

    for excluded_file_index in excluded_files {
        if let Some((_position, is_excluded)) = files.get_mut(excluded_file_index) {
            *is_excluded = true;
        } else {
            return Err(SelectionError::InvalidFileIndex(excluded_file_index));
        }
    }

    let full_piece_len = ctx.pieces.piece_len(0);
    let pieces = (0..ctx.pieces.piece_count()).map(|piece_index| Position {
        global_offset: piece_index * full_piece_len,
        len: ctx.pieces.piece_len(piece_index),
    });

    let mut files = files.into_iter().peekable();
    let mut pieces = pieces.into_iter().enumerate().peekable();

    while let (Some((file, file_excluded)), Some((piece_index, piece))) =
        (files.peek(), pieces.peek())
    {
        if piece.global_offset >= file.global_offset + file.len {
            // piece starts in one of the next files
            files.next();
            continue;
        }

        if piece.global_offset + piece.len <= file.global_offset + file.len {
            // piece ends within the file
            if *file_excluded {
                ctx.piece_tracker.forget_piece(*piece_index);
            }
            pieces.next();
        } else {
            // piece ends outside of the file
            if !*file_excluded {
                pieces.next(); // can't forget this piece even if the next file is excluded
            }
            files.next();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::main::{DownloadStrategy, Mode};
    use mtorrent_base::input;
    use mtorrent_utils::benc::Element;
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn forgotten_pieces(ctx: &ctx::MainCtx) -> Vec<usize> {
        ctx.piece_tracker.missing_pieces_bitfield().iter_zeros().collect()
    }

    fn make_multi_file_ctx(
        file_lengths: &[usize],
        piece_len: usize,
        mode: Mode,
    ) -> ctx::Handle<ctx::MainCtx> {
        let files_list = file_lengths
            .iter()
            .enumerate()
            .map(|(i, &len)| {
                Element::Dictionary(BTreeMap::from([
                    ("length".into(), Element::Integer(len as i64)),
                    ("path".into(), Element::List(vec![format!("file{i}").into()])),
                ]))
            })
            .collect();
        let total_len = file_lengths.iter().sum();
        make_ctx(("files".into(), Element::List(files_list)), total_len, piece_len, mode)
    }

    fn make_single_file_ctx(len: usize, piece_len: usize, mode: Mode) -> ctx::Handle<ctx::MainCtx> {
        make_ctx(("length".into(), Element::Integer(len as i64)), len, piece_len, mode)
    }

    fn make_ctx(
        content_entry: (Element, Element),
        total_len: usize,
        piece_len: usize,
        mode: Mode,
    ) -> ctx::Handle<ctx::MainCtx> {
        let piece_count = total_len.div_ceil(piece_len);
        let info = Element::Dictionary(BTreeMap::from([
            ("name".into(), "test".into()),
            ("piece length".into(), Element::Integer(piece_len as i64)),
            ("pieces".into(), Element::ByteString(vec![0u8; 20 * piece_count])),
            content_entry,
        ]));
        let metainfo = input::Metainfo::from_bencode(info, 0).unwrap();

        ctx::MainCtx::new(
            metainfo,
            [0u8; 20].into(),
            1234,
            12345,
            Ipv4Addr::LOCALHOST,
            Ipv6Addr::LOCALHOST,
            None,
            DownloadStrategy::RarestFirst,
            mode,
        )
        .unwrap()
    }

    #[rstest::rstest]
    #[case::single_excluded_file(&[(30, true)], 10, &[0, 1, 2])]
    #[case::single_included_file(&[(30, false)], 10, &[])]
    #[case::aligned_files(&[(10, true), (10, false), (10, true)], 10, &[0, 2])]
    #[case::piece_spans_excluded_and_included(&[(15, true), (15, false)], 10, &[0])]
    #[case::piece_spans_included_and_excluded(&[(15, false), (15, true)], 10, &[2])]
    #[case::piece_spans_two_excluded(&[(5, true), (5, true), (10, false)], 10, &[0])]
    #[case::piece_spans_three_excluded(&[(3, true), (4, true), (3, true), (10, false)], 10, &[0])]
    #[case::included_file_inside_piece(&[(3, true), (4, false), (3, true), (10, true)], 10, &[1])]
    #[case::large_file_between_excluded(&[(5, true), (30, false), (5, true)], 10, &[])]
    #[case::short_last_piece_excluded(&[(20, false), (5, true)], 10, &[2])]
    #[case::short_last_piece_included(&[(25, false), (5, true)], 10, &[])]
    #[case::empty_file_at_boundary(&[(10, true), (0, false), (10, true)], 10, &[0, 1])]
    #[case::empty_file_at_start(&[(0, false), (10, true)], 10, &[0])]
    #[case::empty_file_at_end(&[(10, true), (0, false)], 10, &[0])]
    #[case::empty_file_inside_piece(&[(5, true), (0, false), (5, true), (10, false)], 10, &[0])]
    #[case::nothing_excluded(&[(7, false), (13, false), (5, false)], 10, &[])]
    #[case::everything_excluded(&[(7, true), (13, true), (5, true)], 10, &[0, 1, 2])]
    fn test_exclude_files(
        #[case] files: &[(usize, bool)],
        #[case] piece_len: usize,
        #[case] expected_forgotten: &[usize],
    ) {
        let file_lengths: Vec<usize> = files.iter().map(|&(len, _)| len).collect();
        let excluded_files: Vec<usize> = files
            .iter()
            .enumerate()
            .filter_map(|(i, &(_, excluded))| excluded.then_some(i))
            .collect();

        let handle = make_multi_file_ctx(&file_lengths, piece_len, Mode::Leech);

        exclude_files(&handle, &excluded_files).unwrap();
        handle.with(|ctx| assert_eq!(forgotten_pieces(ctx), expected_forgotten));
    }

    #[test]
    fn test_invalid_file_index_is_rejected_before_forgetting_anything() {
        let handle = make_multi_file_ctx(&[10, 10, 10], 10, Mode::Leech);
        define_with_ctx!(handle);

        let result = exclude_files(&handle, &[0, 3]);
        assert!(matches!(result, Err(SelectionError::InvalidFileIndex(3))), "{result:?}");
        with_ctx!(|ctx| {
            let forgotten = forgotten_pieces(ctx);
            assert!(forgotten.is_empty(), "unexpected forgotten pieces: {forgotten:?}");
        });
    }

    #[test]
    fn test_single_file_torrent_without_exclusions() {
        let handle = make_single_file_ctx(30, 10, Mode::Leech);
        define_with_ctx!(handle);

        exclude_files(&handle, &[]).unwrap();
        with_ctx!(|ctx| {
            let forgotten = forgotten_pieces(ctx);
            assert!(forgotten.is_empty(), "unexpected forgotten pieces: {forgotten:?}");
        });
    }

    #[test]
    fn test_single_file_torrent_with_exclusions() {
        let handle = make_single_file_ctx(30, 10, Mode::Leech);
        define_with_ctx!(handle);

        let result = exclude_files(&handle, &[0]);
        assert!(matches!(result, Err(SelectionError::SingleFileTorrent)), "{result:?}");
        with_ctx!(|ctx| {
            let forgotten = forgotten_pieces(ctx);
            assert!(forgotten.is_empty(), "unexpected forgotten pieces: {forgotten:?}");
        });
    }

    #[test]
    fn test_seeder_mode_is_rejected() {
        let handle = make_multi_file_ctx(&[10, 10, 10], 10, Mode::Seeder);
        define_with_ctx!(handle);

        let result = exclude_files(&handle, &[0]);
        assert!(matches!(result, Err(SelectionError::UnsupportedMode)), "{result:?}");
        with_ctx!(|ctx| {
            let forgotten = forgotten_pieces(ctx);
            assert!(forgotten.is_empty(), "unexpected forgotten pieces: {forgotten:?}");
        });

        exclude_files(&handle, &[]).unwrap();
    }
}
