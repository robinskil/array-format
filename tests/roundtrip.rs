//! End-to-end tests through the public API: write with `ArrayWriter`, finish,
//! read with `ArrayFile`, reopen from disk, and rewrite a file to change it.

use std::sync::Arc;

use array_format::{
    ArrayFile, ArrayWriter, AttributeValue, Error, FillValue, NoCompression, ReadConfig, StatValue,
    TimestampNs, WriterConfig,
};
use ndarray::{Array, IxDyn};
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};

fn small_writer() -> ArrayWriter {
    ArrayWriter::new(WriterConfig {
        block_target_size: 64,
        ..WriterConfig::new(NoCompression)
    })
}

async fn finish_in_memory(w: ArrayWriter) -> ArrayFile {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    w.finish(store, object_store::path::Path::from("test.af"))
        .await
        .unwrap()
}

fn local_store() -> (tempfile::TempDir, Arc<dyn ObjectStore>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    (dir, store)
}

fn flat<T: Clone>(a: &ndarray::ArcArray<T, IxDyn>) -> Vec<T> {
    a.iter().cloned().collect()
}

// ── Roundtrips ──────────────────────────────────────────────────────

#[tokio::test]
async fn flat_array_roundtrip() {
    let mut w = small_writer();
    w.define_array::<u8>("ints", vec!["x".into()], vec![80], None, None)
        .unwrap();
    w.write_array(
        "ints",
        vec![0],
        Array::from_vec(vec![1u8; 80]).into_dyn().view(),
    )
    .unwrap();
    w.define_array::<f64>("floats", vec!["t".into()], vec![5], None, None)
        .unwrap();
    w.write_array(
        "floats",
        vec![0],
        Array::from_vec(vec![0.0f64; 5]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    assert_eq!(file.arrays().len(), 2);
    let ints = file.read_array::<u8>("ints", vec![], vec![]).await.unwrap();
    assert!(ints.iter().all(|&v| v == 1u8));
    let floats = file
        .read_array::<f64>("floats", vec![], vec![])
        .await
        .unwrap();
    assert!(floats.iter().all(|&v| v == 0.0f64));
}

#[tokio::test]
async fn scalar_array_roundtrip() {
    // 0-D arrays: shape = [], length = 1.
    let mut w = small_writer();

    w.define_array::<f64>("pi", vec![], vec![], None, None)
        .unwrap();
    let pi = Array::from_shape_vec(IxDyn(&[]), vec![std::f64::consts::PI]).unwrap();
    w.write_array("pi", vec![], pi.view()).unwrap();

    // Variable-length scalar: exercises the offset-buffer encoder with N=1.
    w.define_array::<String>("greeting", vec![], vec![], None, None)
        .unwrap();
    let greeting = Array::from_shape_vec(IxDyn(&[]), vec!["hello".to_string()]).unwrap();
    w.write_array("greeting", vec![], greeting.view()).unwrap();

    // Defined but never written, with an explicit fill value.
    w.define_array::<i32>("answer", vec![], vec![], None, Some(FillValue::Int(42)))
        .unwrap();

    let file = finish_in_memory(w).await;

    let pi = file.read_array::<f64>("pi", vec![], vec![]).await.unwrap();
    assert_eq!(pi.ndim(), 0);
    assert_eq!(pi.len(), 1);
    assert_eq!(pi[IxDyn(&[])], std::f64::consts::PI);

    let greeting = file
        .read_array::<String>("greeting", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(greeting.ndim(), 0);
    assert_eq!(greeting[IxDyn(&[])], "hello");

    let answer = file
        .read_array::<i32>("answer", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(answer.ndim(), 0);
    assert_eq!(answer[IxDyn(&[])], 42i32);
}

#[tokio::test]
async fn local_file_roundtrip_and_reopen() {
    let (_dir, store) = local_store();
    let path = object_store::path::Path::from("test.af");

    let mut w = small_writer();
    w.define_array::<f32>("floats", vec!["x".into()], vec![3], None, None)
        .unwrap();
    w.write_array(
        "floats",
        vec![0],
        Array::from_vec(vec![1.0f32, 2.0, 3.0]).into_dyn().view(),
    )
    .unwrap();
    w.set_attribute("floats", "units", AttributeValue::String("m/s".into()))
        .unwrap();
    let from_finish = w.finish(Arc::clone(&store), path.clone()).await.unwrap();

    let reopened = ArrayFile::open(Arc::clone(&store), path.clone(), ReadConfig::default())
        .await
        .unwrap();

    for file in [&from_finish, &reopened] {
        let arr = file
            .read_array::<f32>("floats", vec![], vec![])
            .await
            .unwrap();
        assert_eq!(flat(&arr), vec![1.0f32, 2.0, 3.0]);
        assert_eq!(
            file.get_attribute("floats", "units"),
            Some(&AttributeValue::String("m/s".into()))
        );
    }
}

/// An immutable file changes by being rewritten: open, copy what stays into
/// a new writer, add the rest, finish to a new path.
#[tokio::test]
async fn edit_by_rewrite_drops_and_adds_arrays() {
    let (_dir, store) = local_store();
    let v1 = object_store::path::Path::from("v1.af");
    let v2 = object_store::path::Path::from("v2.af");

    let mut w = small_writer();
    w.define_array::<u8>("a", vec![], vec![20], None, None)
        .unwrap();
    w.write_array(
        "a",
        vec![0],
        Array::from_vec(vec![10u8; 20]).into_dyn().view(),
    )
    .unwrap();
    w.define_array::<u16>("b", vec![], vec![10], None, None)
        .unwrap();
    w.write_array(
        "b",
        vec![0],
        Array::from_vec(vec![20u16; 10]).into_dyn().view(),
    )
    .unwrap();
    w.define_array::<i64>("c", vec![], vec![2], None, None)
        .unwrap();
    w.write_array(
        "c",
        vec![0],
        Array::from_vec(vec![30i64; 2]).into_dyn().view(),
    )
    .unwrap();
    w.finish(Arc::clone(&store), v1.clone()).await.unwrap();

    // Drop "b", add "extra".
    let source = ArrayFile::open(Arc::clone(&store), v1.clone(), ReadConfig::default())
        .await
        .unwrap();
    assert_eq!(source.arrays().len(), 3);
    let mut w = small_writer();
    for info in source.arrays() {
        if info.name.as_str() != "b" {
            w.copy_array(&source, &info.name).await.unwrap();
        }
    }
    w.define_array::<u8>("extra", vec![], vec![4], None, None)
        .unwrap();
    w.write_array(
        "extra",
        vec![0],
        Array::from_vec(vec![7u8; 4]).into_dyn().view(),
    )
    .unwrap();
    w.finish(Arc::clone(&store), v2.clone()).await.unwrap();

    let file = ArrayFile::open(Arc::clone(&store), v2, ReadConfig::default())
        .await
        .unwrap();
    let names: Vec<&str> = file.arrays().iter().map(|a| a.name.as_str()).collect();
    assert_eq!(names, vec!["a", "c", "extra"]);
    let a = file.read_array::<u8>("a", vec![], vec![]).await.unwrap();
    assert!(a.iter().all(|&v| v == 10u8));
    let c = file.read_array::<i64>("c", vec![], vec![]).await.unwrap();
    assert!(c.iter().all(|&v| v == 30i64));
    let extra = file
        .read_array::<u8>("extra", vec![], vec![])
        .await
        .unwrap();
    assert!(extra.iter().all(|&v| v == 7u8));

    // The original is untouched.
    let original = ArrayFile::open(Arc::clone(&store), v1, ReadConfig::default())
        .await
        .unwrap();
    assert_eq!(original.arrays().len(), 3);
}

#[tokio::test]
async fn binary_and_list_attributes_roundtrip() {
    let (_dir, store) = local_store();
    let path = object_store::path::Path::from("attrs.af");

    let mut w = small_writer();
    w.define_array::<f32>("signal", vec!["x".into()], vec![3], None, None)
        .unwrap();
    w.set_attribute(
        "signal",
        "checksum",
        AttributeValue::Binary(Box::new([0xde, 0xad, 0xbe])),
    )
    .unwrap();
    w.set_attribute(
        "signal",
        "coeffs",
        AttributeValue::Float64List(Box::new([0.1, 0.2, 0.3])),
    )
    .unwrap();
    w.set_attribute(
        "signal",
        "tags",
        AttributeValue::StringList(Box::new(["a".into(), "b".into()])),
    )
    .unwrap();
    w.finish(Arc::clone(&store), path.clone()).await.unwrap();

    let file = ArrayFile::open(store, path, ReadConfig::default())
        .await
        .unwrap();
    assert_eq!(
        file.get_attribute("signal", "checksum"),
        Some(&AttributeValue::Binary(Box::new([0xde, 0xad, 0xbe])))
    );
    assert_eq!(
        file.get_attribute("signal", "coeffs"),
        Some(&AttributeValue::Float64List(Box::new([0.1, 0.2, 0.3])))
    );
    assert_eq!(
        file.get_attribute("signal", "tags"),
        Some(&AttributeValue::StringList(Box::new([
            "a".into(),
            "b".into()
        ])))
    );
    assert_eq!(file.attributes("signal").unwrap().count(), 3);
}

// ── write_array / read_array nd tests ───────────────────────────────

#[tokio::test]
async fn write_nd_full_chunks() {
    let mut w = small_writer();
    w.define_array::<i32>(
        "grid",
        vec!["x".into(), "y".into()],
        vec![4, 6],
        Some(vec![2, 3]),
        None,
    )
    .unwrap();
    let data = Array::from_shape_vec(IxDyn(&[4, 6]), (0..24i32).collect::<Vec<_>>()).unwrap();
    w.write_array("grid", vec![0, 0], data.view()).unwrap();
    let file = finish_in_memory(w).await;

    let result = file
        .read_array::<i32>("grid", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(result, data.into_shared());
}

#[tokio::test]
async fn write_nd_partial_chunk() {
    let mut w = small_writer();
    w.define_array::<f32>(
        "g",
        vec!["x".into(), "y".into()],
        vec![4, 4],
        Some(vec![2, 2]),
        None,
    )
    .unwrap();
    let zeros = Array::from_shape_vec(IxDyn(&[4, 4]), vec![0.0f32; 16]).unwrap();
    w.write_array("g", vec![0, 0], zeros.view()).unwrap();
    let patch = Array::from_shape_vec(IxDyn(&[1, 1]), vec![7.0f32]).unwrap();
    w.write_array("g", vec![1, 1], patch.view()).unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<f32>("g", vec![], vec![]).await.unwrap();
    for row in 0..4usize {
        for col in 0..4usize {
            let expected = if row == 1 && col == 1 { 7.0 } else { 0.0 };
            assert_eq!(result[[row, col]], expected);
        }
    }
}

#[tokio::test]
async fn write_nd_multi_chunk_span() {
    let mut w = small_writer();
    w.define_array::<i32>(
        "g",
        vec!["x".into(), "y".into()],
        vec![6, 6],
        Some(vec![3, 3]),
        None,
    )
    .unwrap();
    let ones = Array::from_shape_vec(IxDyn(&[6, 6]), vec![1i32; 36]).unwrap();
    w.write_array("g", vec![0, 0], ones.view()).unwrap();
    let patch = Array::from_shape_vec(IxDyn(&[2, 2]), vec![9i32; 4]).unwrap();
    w.write_array("g", vec![2, 2], patch.view()).unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<i32>("g", vec![], vec![]).await.unwrap();
    for row in 0..6usize {
        for col in 0..6usize {
            let in_patch = (2..4).contains(&row) && (2..4).contains(&col);
            assert_eq!(result[[row, col]], if in_patch { 9 } else { 1 });
        }
    }
}

#[tokio::test]
async fn two_writes_fill_adjacent_chunks() {
    let mut w = small_writer();
    w.define_array::<f32>("data", vec!["x".into()], vec![4], Some(vec![2]), None)
        .unwrap();
    w.write_array(
        "data",
        vec![0],
        Array::from_vec(vec![1.0f32, 2.0]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "data",
        vec![2],
        Array::from_vec(vec![3.0f32, 4.0]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file
        .read_array::<f32>("data", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(flat(&result), vec![1.0, 2.0, 3.0, 4.0]);
}

#[tokio::test]
async fn fill_value_used_for_unwritten_chunks() {
    let mut w = small_writer();
    w.define_array::<i32>(
        "sparse",
        vec!["x".into()],
        vec![6],
        Some(vec![3]),
        Some(FillValue::Int(42)),
    )
    .unwrap();
    // Only the first chunk; [3..6] stays unwritten.
    w.write_array(
        "sparse",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file
        .read_array::<i32>("sparse", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(flat(&result), vec![1, 2, 3, 42, 42, 42]);
}

#[tokio::test]
async fn fill_value_default_zero_when_none() {
    let mut w = small_writer();
    w.define_array::<f64>("empty", vec!["x".into()], vec![4], Some(vec![4]), None)
        .unwrap();
    let file = finish_in_memory(w).await;

    let result = file
        .read_array::<f64>("empty", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(flat(&result), vec![0.0; 4]);
}

#[tokio::test]
async fn read_array_sub_region() {
    let mut w = small_writer();
    w.define_array::<i32>("arr", vec!["x".into()], vec![6], None, None)
        .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![10i32, 20, 30, 40, 50, 60])
            .into_dyn()
            .view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let sub = file
        .read_array::<i32>("arr", vec![2], vec![3])
        .await
        .unwrap();
    assert_eq!(flat(&sub), vec![30, 40, 50]);
}

#[tokio::test]
async fn write_partial_offset_leaves_other_chunks_untouched() {
    // Shape [8], chunk [4], fill 0. Write chunk 0 fully, then indices 5-6 of
    // chunk 1. Indices 4 and 7 must stay 0.
    let mut w = small_writer();
    w.define_array::<i32>(
        "arr",
        vec!["x".into()],
        vec![8],
        Some(vec![4]),
        Some(FillValue::Int(0)),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![5],
        Array::from_vec(vec![10i32, 20]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<i32>("arr", vec![], vec![]).await.unwrap();
    assert_eq!(flat(&result), vec![1, 2, 3, 4, 0, 10, 20, 0]);
}

#[tokio::test]
async fn replace_middle_chunk_leaves_neighbors_intact() {
    let mut w = small_writer();
    w.define_array::<u8>("arr", vec!["x".into()], vec![12], Some(vec![4]), None)
        .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8; 12]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![4],
        Array::from_vec(vec![9u8; 4]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    assert_eq!(flat(&result), vec![1, 1, 1, 1, 9, 9, 9, 9, 1, 1, 1, 1]);
}

#[tokio::test]
async fn cross_chunk_patch_preserves_untouched_elements() {
    // [8] with chunk [4]. All 0s, then [2..6] = 5.
    let mut w = small_writer();
    w.define_array::<i32>(
        "arr",
        vec!["x".into()],
        vec![8],
        Some(vec![4]),
        Some(FillValue::Int(0)),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![0i32; 8]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![2],
        Array::from_vec(vec![5i32; 4]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<i32>("arr", vec![], vec![]).await.unwrap();
    assert_eq!(flat(&result), vec![0, 0, 5, 5, 5, 5, 0, 0]);
}

#[tokio::test]
async fn non_adjacent_chunk_replacement() {
    let mut w = small_writer();
    w.define_array::<u8>("arr", vec!["x".into()], vec![12], Some(vec![4]), None)
        .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8; 12]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![2u8; 4]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![8],
        Array::from_vec(vec![3u8; 4]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    assert_eq!(flat(&result), vec![2, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3]);
}

#[tokio::test]
async fn latest_write_wins_within_a_session() {
    let mut w = small_writer();
    w.define_array::<u8>("arr", vec!["x".into()], vec![4], Some(vec![4]), None)
        .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8; 4]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![2u8; 4]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    assert_eq!(flat(&result), vec![2, 2, 2, 2]);
    assert_eq!(
        file.array("arr").unwrap().written_chunks().count(),
        1,
        "a rewritten chunk keeps one entry"
    );
}

#[tokio::test]
async fn two_d_row_update_spans_column_chunks() {
    // [4,6] with chunk [2,3]. Row 2 spans chunks [1,0] and [1,1].
    let mut w = small_writer();
    w.define_array::<i32>(
        "grid",
        vec!["x".into(), "y".into()],
        vec![4, 6],
        Some(vec![2, 3]),
        None,
    )
    .unwrap();
    w.write_array(
        "grid",
        vec![0, 0],
        Array::from_shape_vec(IxDyn(&[4, 6]), vec![1i32; 24])
            .unwrap()
            .view(),
    )
    .unwrap();
    w.write_array(
        "grid",
        vec![2, 0],
        Array::from_shape_vec(IxDyn(&[1, 6]), vec![7i32; 6])
            .unwrap()
            .view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file
        .read_array::<i32>("grid", vec![], vec![])
        .await
        .unwrap();
    for row in 0..4usize {
        for col in 0..6usize {
            let expected = if row == 2 { 7 } else { 1 };
            assert_eq!(result[[row, col]], expected, "row={row} col={col}");
        }
    }
}

#[tokio::test]
async fn two_d_inner_patch_touches_all_four_chunks() {
    // [4,4] with chunk [2,2]. A [2,2] patch at [1,1] overlaps every chunk.
    let mut w = small_writer();
    w.define_array::<f32>(
        "g",
        vec!["r".into(), "c".into()],
        vec![4, 4],
        Some(vec![2, 2]),
        None,
    )
    .unwrap();
    w.write_array(
        "g",
        vec![0, 0],
        Array::from_shape_vec(IxDyn(&[4, 4]), vec![0.0f32; 16])
            .unwrap()
            .view(),
    )
    .unwrap();
    w.write_array(
        "g",
        vec![1, 1],
        Array::from_shape_vec(IxDyn(&[2, 2]), vec![5.0f32; 4])
            .unwrap()
            .view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let result = file.read_array::<f32>("g", vec![], vec![]).await.unwrap();
    for row in 0..4usize {
        for col in 0..4usize {
            let inside = (1..3).contains(&row) && (1..3).contains(&col);
            assert_eq!(
                result[[row, col]],
                if inside { 5.0 } else { 0.0 },
                "row={row} col={col}"
            );
        }
    }
}

#[tokio::test]
async fn sub_region_read_after_partial_update() {
    // [10] with chunk [5]. 0..10, then [3..7] = 99.
    let mut w = small_writer();
    w.define_array::<i32>("arr", vec!["x".into()], vec![10], Some(vec![5]), None)
        .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec((0..10i32).collect::<Vec<_>>())
            .into_dyn()
            .view(),
    )
    .unwrap();
    w.write_array(
        "arr",
        vec![3],
        Array::from_vec(vec![99i32; 4]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let first = file
        .read_array::<i32>("arr", vec![0], vec![5])
        .await
        .unwrap();
    assert_eq!(flat(&first), vec![0, 1, 2, 99, 99]);
    let second = file
        .read_array::<i32>("arr", vec![5], vec![5])
        .await
        .unwrap();
    assert_eq!(flat(&second), vec![99, 99, 7, 8, 9]);
}

// ── Statistics ──────────────────────────────────────────────────────

#[tokio::test]
async fn stats_finish_computes_min_max_null_row_count() {
    let mut w = small_writer();
    w.define_array::<i32>(
        "data",
        vec!["x".into()],
        vec![6],
        None,
        Some(FillValue::Int(1)),
    )
    .unwrap();
    // [3, 1, 4, 1, 5, 9] with fill 1 -> two nulls, excluded from the range.
    w.write_array(
        "data",
        vec![0],
        Array::from_vec(vec![3i32, 1, 4, 1, 5, 9]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let stats = file
        .array_stats("data")
        .expect("stats missing after finish");
    assert_eq!(stats.min, Some(StatValue::Int(3)));
    assert_eq!(stats.max, Some(StatValue::Int(9)));
    assert_eq!(stats.null_count, 2);
    assert_eq!(stats.row_count, 6);
}

#[tokio::test]
async fn stats_second_chunk_aggregates_globally() {
    let mut w = small_writer();
    w.define_array::<i32>("a", vec!["x".into()], vec![10], Some(vec![5]), None)
        .unwrap();
    w.write_array(
        "a",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4, 5]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "a",
        vec![5],
        Array::from_vec(vec![6i32, 7, 8, 9, 10]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let stats = file.array_stats("a").expect("stats missing");
    assert_eq!(stats.min, Some(StatValue::Int(1)));
    assert_eq!(stats.max, Some(StatValue::Int(10)));
    assert_eq!(stats.row_count, 10);
    assert_eq!(stats.null_count, 0);
}

#[tokio::test]
async fn stats_update_after_chunk_overwrite() {
    let mut w = small_writer();
    w.define_array::<i32>("x", vec!["i".into()], vec![4], None, None)
        .unwrap();
    w.write_array(
        "x",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn().view(),
    )
    .unwrap();
    w.write_array(
        "x",
        vec![0],
        Array::from_vec(vec![10i32, 20, 30, 40]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let stats = file.array_stats("x").expect("stats missing");
    assert_eq!(stats.min, Some(StatValue::Int(10)));
    assert_eq!(stats.max, Some(StatValue::Int(40)));
    assert_eq!(stats.row_count, 4);
}

#[tokio::test]
async fn stats_loaded_on_open() {
    let (_dir, store) = local_store();
    let path = object_store::path::Path::from("data.af");

    let mut w = small_writer();
    w.define_array::<i32>("nums", vec!["i".into()], vec![3], None, None)
        .unwrap();
    w.write_array(
        "nums",
        vec![0],
        Array::from_vec(vec![7i32, 2, 5]).into_dyn().view(),
    )
    .unwrap();
    w.finish(Arc::clone(&store), path.clone()).await.unwrap();

    let file = ArrayFile::open(store, path, ReadConfig::default())
        .await
        .unwrap();
    let stats = file.array_stats("nums").expect("stats not loaded on open");
    assert_eq!(stats.min, Some(StatValue::Int(2)));
    assert_eq!(stats.max, Some(StatValue::Int(7)));
    assert_eq!(stats.row_count, 3);
}

#[tokio::test]
async fn stats_unwritten_chunks_count_as_nulls() {
    let mut w = small_writer();
    w.define_array::<i32>(
        "partial",
        vec!["x".into()],
        vec![10],
        Some(vec![5]),
        Some(FillValue::Int(0)),
    )
    .unwrap();
    w.write_array(
        "partial",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4, 5]).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let stats = file.array_stats("partial").expect("stats missing");
    assert_eq!(stats.row_count, 10);
    assert_eq!(stats.null_count, 5);
    assert_eq!(stats.min, Some(StatValue::Int(1)));
    assert_eq!(stats.max, Some(StatValue::Int(5)));
}

#[tokio::test]
async fn timestamp_ns_roundtrip_and_stats() {
    let fill = 1_000_000_000i64;
    let values = vec![
        TimestampNs(0),
        TimestampNs(fill),
        TimestampNs(2_000_000_000),
        TimestampNs(-500),
        TimestampNs(fill),
    ];
    let mut w = small_writer();
    w.define_array::<TimestampNs>(
        "events",
        vec!["t".into()],
        vec![values.len()],
        None,
        Some(FillValue::TimestampNs(fill)),
    )
    .unwrap();
    w.write_array(
        "events",
        vec![0],
        Array::from_vec(values.clone()).into_dyn().view(),
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let back = file
        .read_array::<TimestampNs>("events", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(flat(&back), values);

    let stats = file.array_stats("events").expect("stats missing");
    assert_eq!(stats.min, Some(StatValue::TimestampNs(-500)));
    assert_eq!(stats.max, Some(StatValue::TimestampNs(2_000_000_000)));
    assert_eq!(stats.null_count, 2);
    assert_eq!(stats.row_count, values.len() as u64);
}

#[tokio::test]
async fn stats_iterator_exposes_every_array_in_file_order() {
    let mut w = small_writer();
    for name in ["c", "a", "b"] {
        w.define_array::<i32>(name, vec!["x".into()], vec![2], None, None)
            .unwrap();
        w.write_array(
            name,
            vec![0],
            Array::from_vec(vec![1i32, 2]).into_dyn().view(),
        )
        .unwrap();
    }
    let file = finish_in_memory(w).await;

    let names: Vec<&str> = file.stats().map(|s| s.name.as_str()).collect();
    assert_eq!(names, vec!["c", "a", "b"]);
}

// ── Zero-length dimensions ──────────────────────────────────────────

/// A zero-length axis makes the array empty. NetCDF declares such a dimension
/// whenever the records it holds are absent.
#[tokio::test]
async fn zero_length_dimension_roundtrip() {
    let mut w = small_writer();
    w.define_array::<f32>(
        "history",
        vec!["n_history".into(), "x".into()],
        vec![0, 3],
        None,
        None,
    )
    .unwrap();
    let data = Array::from_shape_vec(IxDyn(&[0, 3]), Vec::<f32>::new()).unwrap();
    w.write_array("history", vec![0, 0], data.view()).unwrap();
    let file = finish_in_memory(w).await;

    // The chunk extent of an empty axis is stored as 1, never 0.
    let info = file.array("history").expect("array is visible");
    assert_eq!(info.shape, vec![0, 3]);
    assert_eq!(info.chunk_shape, vec![1, 3]);

    let out = file
        .read_array::<f32>("history", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(out.shape(), &[0, 3]);
    assert_eq!(out.len(), 0);
}

/// A converter that mirrors the shape into the chunk shape passes a zero
/// extent for the empty axis. That is accepted and normalized to 1.
#[tokio::test]
async fn zero_length_dimension_with_mirrored_chunk_shape() {
    let mut w = small_writer();
    w.define_array::<i32>(
        "history",
        vec!["n_history".into(), "x".into()],
        vec![0, 3],
        Some(vec![0, 3]),
        None,
    )
    .unwrap();
    let data = Array::from_shape_vec(IxDyn(&[0, 3]), Vec::<i32>::new()).unwrap();
    w.write_array("history", vec![0, 0], data.view()).unwrap();
    let file = finish_in_memory(w).await;

    let out = file
        .read_array::<i32>("history", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(out.shape(), &[0, 3]);
}

#[tokio::test]
async fn zero_length_middle_axis_reads_back_empty() {
    let mut w = small_writer();
    w.define_array::<f64>(
        "mid",
        vec!["a".into(), "b".into(), "c".into()],
        vec![2, 0, 4],
        Some(vec![1, 0, 2]),
        None,
    )
    .unwrap();
    let file = finish_in_memory(w).await;

    let out = file.read_array::<f64>("mid", vec![], vec![]).await.unwrap();
    assert_eq!(out.shape(), &[2, 0, 4]);
    assert_eq!(out.len(), 0);
}

/// An empty view is a no-op, even on an array that holds data.
#[tokio::test]
async fn empty_write_leaves_a_populated_array_untouched() {
    let mut w = small_writer();
    w.define_array::<u8>("arr", vec!["x".into()], vec![4], Some(vec![2]), None)
        .unwrap();
    w.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8, 2, 3, 4]).into_dyn().view(),
    )
    .unwrap();
    let empty = Array::from_shape_vec(IxDyn(&[0]), Vec::<u8>::new()).unwrap();
    w.write_array("arr", vec![2], empty.view()).unwrap();
    let file = finish_in_memory(w).await;

    let out = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    assert_eq!(flat(&out), vec![1, 2, 3, 4]);
}

/// An empty array holds no elements, so any non-empty write is out of range.
#[tokio::test]
async fn write_to_zero_length_axis_reports_out_of_range() {
    let mut w = small_writer();
    w.define_array::<f32>("history", vec!["n".into()], vec![0], None, None)
        .unwrap();
    let err = w
        .write_array(
            "history",
            vec![0],
            Array::from_vec(vec![1.0f32]).into_dyn().view(),
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("exceeds array size"),
        "unexpected error: {err}"
    );
}

/// A zero chunk extent on a non-empty axis has no meaning. It is rejected at
/// definition time, rather than dividing by zero on the first write.
#[tokio::test]
async fn zero_chunk_extent_on_a_non_empty_axis_is_rejected() {
    let mut w = small_writer();
    let err = w
        .define_array::<f32>("bad", vec!["x".into()], vec![4], Some(vec![0]), None)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidChunkShape { .. }));
    let file = finish_in_memory(w).await;
    assert!(file.arrays().is_empty(), "nothing was defined");
}

#[tokio::test]
async fn chunk_shape_with_wrong_axis_count_is_rejected() {
    let mut w = small_writer();
    let err = w
        .define_array::<f32>(
            "bad",
            vec!["x".into(), "y".into()],
            vec![4, 4],
            Some(vec![2]),
            None,
        )
        .unwrap_err();
    assert!(matches!(err, Error::InvalidChunkShape { .. }));
}

/// The empty axis survives the footer: define, finish, reopen, read.
#[tokio::test]
async fn zero_length_dimension_survives_reopen() {
    let (_dir, store) = local_store();
    let path = object_store::path::Path::from("history.af");

    let mut w = small_writer();
    w.define_array::<f32>(
        "history",
        vec!["n_history".into(), "x".into()],
        vec![0, 3],
        None,
        None,
    )
    .unwrap();
    let data = Array::from_shape_vec(IxDyn(&[0, 3]), Vec::<f32>::new()).unwrap();
    w.write_array("history", vec![0, 0], data.view()).unwrap();
    w.set_attribute("history", "units", AttributeValue::String("n/a".into()))
        .unwrap();
    w.finish(Arc::clone(&store), path.clone()).await.unwrap();

    let file = ArrayFile::open(store, path, ReadConfig::default())
        .await
        .unwrap();
    let info = file.array("history").expect("array survives the reopen");
    assert_eq!(info.shape, vec![0, 3]);
    assert_eq!(info.dimension_names, vec!["n_history", "x"]);
    let out = file
        .read_array::<f32>("history", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(out.shape(), &[0, 3]);
    assert_eq!(
        file.get_attribute("history", "units"),
        Some(&AttributeValue::String("n/a".into()))
    );
}

// ── Attributes across a rewrite ─────────────────────────────────────

/// A rewrite that copies most arrays, overrides one attribute, drops one
/// array and adds another. The column reflects the new file exactly.
#[tokio::test]
async fn attribute_index_over_copied_and_added_arrays() {
    let mut w = small_writer();
    for i in 0..8 {
        w.define_array::<f64>(format!("a{i}"), vec!["t".into()], vec![4], None, None)
            .unwrap();
        w.set_attribute(&format!("a{i}"), "units", AttributeValue::Int64(i))
            .unwrap();
    }
    let source = finish_in_memory(w).await;

    let mut w = small_writer();
    for info in source.arrays() {
        if info.name.as_str() != "a5" {
            w.copy_array(&source, &info.name).await.unwrap();
        }
    }
    w.set_attribute("a2", "units", AttributeValue::Int64(222))
        .unwrap();
    w.define_array::<f64>("a8", vec!["t".into()], vec![4], None, None)
        .unwrap();
    w.set_attribute("a8", "units", AttributeValue::Int64(888))
        .unwrap();
    let file = finish_in_memory(w).await;

    let column: Vec<(String, Option<AttributeValue>)> = file
        .attribute_index("units")
        .into_iter()
        .map(|(n, v)| (n.to_string(), v.cloned()))
        .collect();
    assert_eq!(column.len(), 8, "a5 is gone, a8 is new");
    let get = |name: &str| {
        column
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(get("a0"), Some(Some(AttributeValue::Int64(0))));
    assert_eq!(get("a2"), Some(Some(AttributeValue::Int64(222))));
    assert_eq!(get("a5"), None);
    assert_eq!(get("a8"), Some(Some(AttributeValue::Int64(888))));
    // File order: copied arrays first, in source order, then the addition.
    assert_eq!(column[0].0, "a0");
    assert_eq!(column[7].0, "a8");
}

/// A key that no array carries yields a column of `None`, one per array.
#[tokio::test]
async fn attribute_index_unknown_key_spans_every_array() {
    let mut w = small_writer();
    for i in 0..3 {
        w.define_array::<f64>(format!("a{i}"), vec!["t".into()], vec![4], None, None)
            .unwrap();
        w.set_attribute(&format!("a{i}"), "units", AttributeValue::Int64(i))
            .unwrap();
    }
    let file = finish_in_memory(w).await;

    let column = file.attribute_index("no_such_key");
    assert_eq!(column.len(), 3);
    assert!(column.iter().all(|(_, v)| v.is_none()));
}
