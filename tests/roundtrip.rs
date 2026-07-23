//! End-to-end roundtrip tests: write → read → delete → compact → re-read.

use std::sync::Arc;

use array_format::{
    ArrayFile, AttributeValue, FileConfig, FillValue, NoCompression, StatValue, TimestampNs,
};
use ndarray::{Array, IxDyn};
use object_store::{ObjectStore, local::LocalFileSystem};

fn small_config() -> FileConfig<NoCompression> {
    FileConfig {
        block_target_size: 64,
        ..FileConfig::new(NoCompression)
    }
}

#[tokio::test]
async fn flat_array_roundtrip() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();

    file.define_array::<u8>("ints", vec!["x".into()], vec![80], None, None)
        .unwrap();
    let ints = Array::from_vec(vec![1u8; 80]).into_dyn();
    file.write_array("ints", vec![0], ints.view())
        .await
        .unwrap();

    file.define_array::<f64>("floats", vec!["t".into()], vec![5], None, None)
        .unwrap();
    let floats = Array::from_vec(vec![0.0f64; 5]).into_dyn();
    file.write_array("floats", vec![0], floats.view())
        .await
        .unwrap();

    file.flush().await.unwrap();

    assert_eq!(file.list_arrays().len(), 2);
    let ints_back = file.read_array::<u8>("ints", vec![], vec![]).await.unwrap();
    assert!(ints_back.iter().all(|&v| v == 1u8));
    let floats_back = file
        .read_array::<f64>("floats", vec![], vec![])
        .await
        .unwrap();
    assert!(floats_back.iter().all(|&v| v == 0.0f64));
}

#[tokio::test]
async fn scalar_array_roundtrip() {
    // 0-D arrays: shape = [], length = 1.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();

    // Fixed-width scalar (f64).
    file.define_array::<f64>("pi", vec![], vec![], None, None)
        .unwrap();
    let pi = Array::from_shape_vec(IxDyn(&[]), vec![std::f64::consts::PI]).unwrap();
    file.write_array("pi", vec![], pi.view()).await.unwrap();

    // Variable-length scalar (String) — exercises the offset-buffer encoder with N=1.
    file.define_array::<String>("greeting", vec![], vec![], None, None)
        .unwrap();
    let greeting = Array::from_shape_vec(IxDyn(&[]), vec!["hello".to_string()]).unwrap();
    file.write_array("greeting", vec![], greeting.view())
        .await
        .unwrap();

    // Defined-but-unwritten scalar with an explicit fill_value.
    file.define_array::<i32>("answer", vec![], vec![], None, Some(FillValue::Int(42)))
        .unwrap();

    file.flush().await.unwrap();

    let pi_back = file.read_array::<f64>("pi", vec![], vec![]).await.unwrap();
    assert_eq!(pi_back.ndim(), 0);
    assert_eq!(pi_back.len(), 1);
    assert_eq!(pi_back[IxDyn(&[])], std::f64::consts::PI);

    let greeting_back = file
        .read_array::<String>("greeting", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(greeting_back.ndim(), 0);
    assert_eq!(greeting_back[IxDyn(&[])], "hello");

    let answer_back = file
        .read_array::<i32>("answer", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(answer_back.ndim(), 0);
    assert_eq!(answer_back[IxDyn(&[])], 42i32);
}

#[tokio::test]
async fn delete_and_compact() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("test.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<u8>("a", vec![], vec![20], None, None)
            .unwrap();
        file.write_array(
            "a",
            vec![0],
            Array::from_vec(vec![10u8; 20]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.define_array::<u16>("b", vec![], vec![10], None, None)
            .unwrap();
        file.write_array(
            "b",
            vec![0],
            Array::from_vec(vec![20u16; 10]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.define_array::<i64>("c", vec![], vec![2], None, None)
            .unwrap();
        file.write_array(
            "c",
            vec![0],
            Array::from_vec(vec![30i64; 2]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        assert_eq!(file.list_arrays().len(), 3);
        file.delete("b").unwrap();
        file.flush().await.unwrap();
        assert_eq!(file.list_arrays().len(), 2);
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        assert_eq!(file.list_arrays().len(), 2);
        file.compact().await.unwrap();
        assert_eq!(file.num_layers(), 1);

        let mut names: Vec<_> = file.list_arrays().iter().map(|a| a.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["a", "c"]);
        let a = file.read_array::<u8>("a", vec![], vec![]).await.unwrap();
        assert!(a.iter().all(|&v| v == 10u8));
        let c = file.read_array::<i64>("c", vec![], vec![]).await.unwrap();
        assert!(c.iter().all(|&v| v == 30i64));
    }
}

#[tokio::test]
async fn local_file_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("test.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<f32>("floats", vec!["x".into()], vec![3], None, None)
            .unwrap();
        let arr = Array::from_vec(vec![1.0f32, 2.0, 3.0]).into_dyn();
        file.write_array("floats", vec![0], arr.view())
            .await
            .unwrap();
        file.set_attribute("floats", "units", AttributeValue::String("m/s".into()))
            .unwrap();
        file.flush().await.unwrap();
    }

    {
        let file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        let arr = file
            .read_array::<f32>("floats", vec![], vec![])
            .await
            .unwrap();
        let flat: Vec<f32> = arr.iter().cloned().collect();
        assert_eq!(flat, &[1.0f32, 2.0, 3.0]);
        let v = file.get_attribute("floats", "units").unwrap();
        assert_eq!(v, Some(&AttributeValue::String("m/s".into())));
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<u8>("extra", vec![], vec![4], None, None)
            .unwrap();
        let extra = Array::from_vec(vec![7u8; 4]).into_dyn();
        file.write_array("extra", vec![0], extra.view())
            .await
            .unwrap();
        file.flush().await.unwrap();
    }

    {
        let file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        assert_eq!(file.list_arrays().len(), 2);
        let extra = file
            .read_array::<u8>("extra", vec![], vec![])
            .await
            .unwrap();
        assert!(extra.iter().all(|&v| v == 7u8));
    }
}

#[tokio::test]
async fn binary_and_list_attributes_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("attrs.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<f32>("signal", vec!["x".into()], vec![3], None, None)
            .unwrap();
        file.set_attribute(
            "signal",
            "checksum",
            AttributeValue::Binary(vec![0xde, 0xad, 0xbe]),
        )
        .unwrap();
        file.set_attribute(
            "signal",
            "coeffs",
            AttributeValue::Float64List(vec![0.1, 0.2, 0.3]),
        )
        .unwrap();
        file.set_attribute(
            "signal",
            "tags",
            AttributeValue::StringList(vec!["a".into(), "b".into()]),
        )
        .unwrap();
        file.flush().await.unwrap();
    }

    {
        let file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        assert_eq!(
            file.get_attribute("signal", "checksum").unwrap(),
            Some(&AttributeValue::Binary(vec![0xde, 0xad, 0xbe]))
        );
        assert_eq!(
            file.get_attribute("signal", "coeffs").unwrap(),
            Some(&AttributeValue::Float64List(vec![0.1, 0.2, 0.3]))
        );
        assert_eq!(
            file.get_attribute("signal", "tags").unwrap(),
            Some(&AttributeValue::StringList(vec!["a".into(), "b".into()]))
        );
    }
}

#[tokio::test]
async fn layered_flat_write() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("layered.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<u8>("a", vec!["x".into()], vec![3], None, None)
            .unwrap();
        file.write_array(
            "a",
            vec![0],
            Array::from_vec(vec![1u8, 2, 3]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<u8>("b", vec!["x".into()], vec![3], None, None)
            .unwrap();
        file.write_array(
            "b",
            vec![0],
            Array::from_vec(vec![4u8, 5, 6]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
    }

    let file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
        .await
        .unwrap();
    assert_eq!(file.num_layers(), 3);
    let a = file.read_array::<u8>("a", vec![], vec![]).await.unwrap();
    let a_flat: Vec<u8> = a.iter().cloned().collect();
    assert_eq!(a_flat, &[1, 2, 3]);
    let b = file.read_array::<u8>("b", vec![], vec![]).await.unwrap();
    let b_flat: Vec<u8> = b.iter().cloned().collect();
    assert_eq!(b_flat, &[4, 5, 6]);
}

#[tokio::test]
async fn layered_chunk_update() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("chunks.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<f32>(
            "grid",
            vec!["x".into(), "y".into()],
            vec![4, 4],
            Some(vec![2, 2]),
            None,
        )
        .unwrap();
        let chunk = Array::from_shape_vec(IxDyn(&[2, 2]), vec![1.0f32; 4]).unwrap();
        for cr in 0..2usize {
            for cc in 0..2usize {
                file.write_array("grid", vec![cr * 2, cc * 2], chunk.view())
                    .await
                    .unwrap();
            }
        }
        file.flush().await.unwrap();
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        let new_chunk = Array::from_shape_vec(IxDyn(&[2, 2]), vec![9.0f32; 4]).unwrap();
        file.write_array("grid", vec![2, 2], new_chunk.view())
            .await
            .unwrap();
        file.flush().await.unwrap();
    }

    let file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
        .await
        .unwrap();
    let c00 = file
        .read_array::<f32>("grid", vec![0, 0], vec![2, 2])
        .await
        .unwrap();
    assert!(c00.iter().all(|&v| v == 1.0f32));
    let c11 = file
        .read_array::<f32>("grid", vec![2, 2], vec![2, 2])
        .await
        .unwrap();
    assert!(c11.iter().all(|&v| v == 9.0f32));
}

#[tokio::test]
async fn compact_merges_layers() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("compact.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<u8>("a", vec![], vec![3], None, None)
            .unwrap();
        file.write_array(
            "a",
            vec![0],
            Array::from_vec(vec![1u8, 2, 3]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
        file.define_array::<u8>("b", vec![], vec![3], None, None)
            .unwrap();
        file.write_array(
            "b",
            vec![0],
            Array::from_vec(vec![4u8, 5, 6]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
        assert_eq!(file.num_layers(), 3);
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        assert_eq!(file.num_layers(), 3);
        file.compact().await.unwrap();
        assert_eq!(file.num_layers(), 1);
        assert_eq!(file.list_arrays().len(), 2);
        let a = file.read_array::<u8>("a", vec![], vec![]).await.unwrap();
        let a_flat: Vec<u8> = a.iter().cloned().collect();
        assert_eq!(a_flat, &[1, 2, 3]);
        let b = file.read_array::<u8>("b", vec![], vec![]).await.unwrap();
        let b_flat: Vec<u8> = b.iter().cloned().collect();
        assert_eq!(b_flat, &[4, 5, 6]);
    }
}

#[tokio::test]
async fn delete_in_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("del.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<u8>("arr", vec![], vec![1], None, None)
            .unwrap();
        file.write_array("arr", vec![0], Array::from_vec(vec![1u8]).into_dyn().view())
            .await
            .unwrap();
        file.flush().await.unwrap();
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.delete("arr").unwrap();
        file.flush().await.unwrap();
    }

    let file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
        .await
        .unwrap();
    assert!(file.get_array("arr").is_err());
    assert_eq!(file.list_arrays().len(), 0);
}

// ── write_array / read_array nd tests ───────────────────────────────

#[tokio::test]
async fn write_nd_full_chunks() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "grid",
        vec!["x".into(), "y".into()],
        vec![4, 6],
        Some(vec![2, 3]),
        None,
    )
    .unwrap();

    let data = Array::from_shape_vec(IxDyn(&[4, 6]), (0..24i32).collect::<Vec<_>>()).unwrap();
    file.write_array("grid", vec![0, 0], data.view())
        .await
        .unwrap();

    file.flush().await.unwrap();

    let result = file
        .read_array::<i32>("grid", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(result, data.into_shared());
}

#[tokio::test]
async fn write_nd_partial_chunk() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<f32>(
        "g",
        vec!["x".into(), "y".into()],
        vec![4, 4],
        Some(vec![2, 2]),
        None,
    )
    .unwrap();
    let zeros = Array::from_shape_vec(IxDyn(&[4, 4]), vec![0.0f32; 16]).unwrap();
    file.write_array("g", vec![0, 0], zeros.view())
        .await
        .unwrap();
    file.flush().await.unwrap();

    let patch = Array::from_shape_vec(IxDyn(&[1, 1]), vec![7.0f32]).unwrap();
    file.write_array("g", vec![1, 1], patch.view())
        .await
        .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<f32>("g", vec![], vec![]).await.unwrap();
    for row in 0..4usize {
        for col in 0..4usize {
            let val = result[[row, col]];
            if row == 1 && col == 1 {
                assert_eq!(val, 7.0);
            } else {
                assert_eq!(val, 0.0);
            }
        }
    }
}

#[tokio::test]
async fn write_nd_multi_chunk_span() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "g",
        vec!["x".into(), "y".into()],
        vec![6, 6],
        Some(vec![3, 3]),
        None,
    )
    .unwrap();
    let ones = Array::from_shape_vec(IxDyn(&[6, 6]), vec![1i32; 36]).unwrap();
    file.write_array("g", vec![0, 0], ones.view())
        .await
        .unwrap();
    file.flush().await.unwrap();

    let patch = Array::from_shape_vec(IxDyn(&[2, 2]), vec![9i32; 4]).unwrap();
    file.write_array("g", vec![2, 2], patch.view())
        .await
        .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<i32>("g", vec![], vec![]).await.unwrap();
    for row in 0..6usize {
        for col in 0..6usize {
            let val = result[[row, col]];
            let in_patch = (2..4).contains(&row) && (2..4).contains(&col);
            if in_patch {
                assert_eq!(val, 9);
            } else {
                assert_eq!(val, 1);
            }
        }
    }
}

#[tokio::test]
async fn write_nd_pending_array() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<f32>("data", vec!["x".into()], vec![4], Some(vec![2]), None)
        .unwrap();

    let a = Array::from_vec(vec![1.0f32, 2.0]).into_dyn();
    file.write_array("data", vec![0], a.view()).await.unwrap();
    let b = Array::from_vec(vec![3.0f32, 4.0]).into_dyn();
    file.write_array("data", vec![2], b.view()).await.unwrap();

    file.flush().await.unwrap();

    let result = file
        .read_array::<f32>("data", vec![], vec![])
        .await
        .unwrap();
    let flat: Vec<f32> = result.iter().cloned().collect();
    assert_eq!(flat, vec![1.0, 2.0, 3.0, 4.0]);
}

#[tokio::test]
async fn fill_value_used_for_unwritten_chunks() {
    use array_format::FillValue;

    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();

    // i32 array with fill value 42, chunked so not every chunk needs to be written.
    file.define_array::<i32>(
        "sparse",
        vec!["x".into()],
        vec![6],
        Some(vec![3]),
        Some(FillValue::Int(42)),
    )
    .unwrap();

    // Write only the first chunk; second chunk [3..6] stays unwritten.
    let first = Array::from_vec(vec![1i32, 2, 3]).into_dyn();
    file.write_array("sparse", vec![0], first.view())
        .await
        .unwrap();

    file.flush().await.unwrap();

    let result = file
        .read_array::<i32>("sparse", vec![], vec![])
        .await
        .unwrap();
    let flat: Vec<i32> = result.iter().cloned().collect();
    // First chunk as written; second chunk filled with 42.
    assert_eq!(flat, vec![1, 2, 3, 42, 42, 42]);
}

#[tokio::test]
async fn fill_value_default_zero_when_none() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();

    // f64 array with no explicit fill value — should read as 0.0.
    file.define_array::<f64>("empty", vec!["x".into()], vec![4], Some(vec![4]), None)
        .unwrap();

    file.flush().await.unwrap();

    let result = file
        .read_array::<f64>("empty", vec![], vec![])
        .await
        .unwrap();
    let flat: Vec<f64> = result.iter().cloned().collect();
    assert_eq!(flat, vec![0.0; 4]);
}

#[tokio::test]
async fn read_array_sub_region() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>("arr", vec!["x".into()], vec![6], None, None)
        .unwrap();
    let data = Array::from_vec(vec![10i32, 20, 30, 40, 50, 60]).into_dyn();
    file.write_array("arr", vec![0], data.view()).await.unwrap();
    file.flush().await.unwrap();

    // Read elements [2..5]
    let sub = file
        .read_array::<i32>("arr", vec![2], vec![3])
        .await
        .unwrap();
    let flat: Vec<i32> = sub.iter().cloned().collect();
    assert_eq!(flat, vec![30, 40, 50]);
}

#[tokio::test]
async fn write_partial_offset_leaves_other_chunks_untouched() {
    // Shape=[8], chunk_shape=[4], fill_value=0.
    // Write chunk 0 fully (indices 0-3), then partially update chunk 1 (indices 5-6).
    // After flush+read the untouched index 4 and 7 must remain 0.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "arr",
        vec!["x".into()],
        vec![8],
        Some(vec![4]),
        Some(array_format::FillValue::Int(0)),
    )
    .unwrap();

    let chunk0 = Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn();
    file.write_array("arr", vec![0], chunk0.view())
        .await
        .unwrap();

    let patch = Array::from_vec(vec![10i32, 20]).into_dyn();
    file.write_array("arr", vec![5], patch.view())
        .await
        .unwrap();

    file.flush().await.unwrap();

    let result = file.read_array::<i32>("arr", vec![], vec![]).await.unwrap();
    let flat: Vec<i32> = result.iter().cloned().collect();
    assert_eq!(flat, vec![1, 2, 3, 4, 0, 10, 20, 0]);
}

#[tokio::test]
async fn replace_middle_chunk_leaves_neighbors_intact() {
    // [12] with chunk=[4]: chunks at [0], [4], [8].
    // Write all 1s, flush. Overwrite middle chunk with 9s, flush.
    // Expect: [1,1,1,1, 9,9,9,9, 1,1,1,1].
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<u8>("arr", vec!["x".into()], vec![12], Some(vec![4]), None)
        .unwrap();
    file.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8; 12]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "arr",
        vec![4],
        Array::from_vec(vec![9u8; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    let flat: Vec<u8> = result.iter().cloned().collect();
    assert_eq!(flat, vec![1, 1, 1, 1, 9, 9, 9, 9, 1, 1, 1, 1]);
}

#[tokio::test]
async fn cross_chunk_patch_preserves_untouched_elements() {
    // [8] with chunk=[4]. Write all 0s, flush. Patch indices [2..6] with 5s, flush.
    // Chunk 0 [0..4]: indices 0,1 stay 0; indices 2,3 become 5.
    // Chunk 1 [4..8]: indices 4,5 become 5; indices 6,7 stay 0.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "arr",
        vec!["x".into()],
        vec![8],
        Some(vec![4]),
        Some(array_format::FillValue::Int(0)),
    )
    .unwrap();
    file.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![0i32; 8]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "arr",
        vec![2],
        Array::from_vec(vec![5i32; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<i32>("arr", vec![], vec![]).await.unwrap();
    let flat: Vec<i32> = result.iter().cloned().collect();
    assert_eq!(flat, vec![0, 0, 5, 5, 5, 5, 0, 0]);
}

#[tokio::test]
async fn non_adjacent_chunk_replacement() {
    // [12] with chunk=[4]. Write all 1s. Replace first and last chunk in one overlay.
    // Middle chunk must remain 1.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<u8>("arr", vec!["x".into()], vec![12], Some(vec![4]), None)
        .unwrap();
    file.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8; 12]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![2u8; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.write_array(
        "arr",
        vec![8],
        Array::from_vec(vec![3u8; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    let flat: Vec<u8> = result.iter().cloned().collect();
    assert_eq!(flat, vec![2, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3]);
}

#[tokio::test]
async fn latest_write_wins_across_layers() {
    // Write a chunk with value 1, flush. Overwrite same chunk with value 2, flush.
    // Read must return 2, not 1.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<u8>("arr", vec!["x".into()], vec![4], Some(vec![4]), None)
        .unwrap();
    file.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![1u8; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "arr",
        vec![0],
        Array::from_vec(vec![2u8; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<u8>("arr", vec![], vec![]).await.unwrap();
    let flat: Vec<u8> = result.iter().cloned().collect();
    assert_eq!(flat, vec![2, 2, 2, 2]);
}

#[tokio::test]
async fn compact_preserves_partial_updates() {
    // Write 12 elements in 3 chunks. Update middle chunk. Compact. Verify merged result.
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap()) as Arc<dyn ObjectStore>;
    let path = object_store::path::Path::from("compact_partial.af");

    {
        let mut file = ArrayFile::create(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<i32>("arr", vec!["x".into()], vec![12], Some(vec![4]), None)
            .unwrap();
        file.write_array(
            "arr",
            vec![0],
            Array::from_vec(vec![1i32; 12]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.write_array(
            "arr",
            vec![4],
            Array::from_vec(vec![9i32; 4]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
    }

    {
        let mut file = ArrayFile::open(Arc::clone(&store), path.clone(), small_config())
            .await
            .unwrap();
        file.compact().await.unwrap();
        assert_eq!(file.num_layers(), 1);
        let result = file.read_array::<i32>("arr", vec![], vec![]).await.unwrap();
        let flat: Vec<i32> = result.iter().cloned().collect();
        assert_eq!(flat, vec![1, 1, 1, 1, 9, 9, 9, 9, 1, 1, 1, 1]);
    }
}

#[tokio::test]
async fn two_d_row_update_spans_column_chunks() {
    // [4,6] with chunk [2,3]. Write all 1s, flush. Update row 2 entirely, flush.
    // Row 2 spans chunk [2,0] and chunk [2,3]. Rows 0,1,3 must remain 1.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "grid",
        vec!["x".into(), "y".into()],
        vec![4, 6],
        Some(vec![2, 3]),
        None,
    )
    .unwrap();
    file.write_array(
        "grid",
        vec![0, 0],
        Array::from_shape_vec(IxDyn(&[4, 6]), vec![1i32; 24])
            .unwrap()
            .view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "grid",
        vec![2, 0],
        Array::from_shape_vec(IxDyn(&[1, 6]), vec![7i32; 6])
            .unwrap()
            .view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let result = file
        .read_array::<i32>("grid", vec![], vec![])
        .await
        .unwrap();
    for row in 0..4usize {
        for col in 0..6usize {
            let val = result[[row, col]];
            if row == 2 {
                assert_eq!(val, 7, "row={row} col={col}");
            } else {
                assert_eq!(val, 1, "row={row} col={col}");
            }
        }
    }
}

#[tokio::test]
async fn two_d_inner_patch_touches_all_four_chunks() {
    // [4,4] with chunk [2,2]. Write all 0s, flush. Patch inner [2,2] at [1,1], flush.
    // The patch at rows 1-2, cols 1-2 overlaps all four chunks.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<f32>(
        "g",
        vec!["r".into(), "c".into()],
        vec![4, 4],
        Some(vec![2, 2]),
        None,
    )
    .unwrap();
    file.write_array(
        "g",
        vec![0, 0],
        Array::from_shape_vec(IxDyn(&[4, 4]), vec![0.0f32; 16])
            .unwrap()
            .view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "g",
        vec![1, 1],
        Array::from_shape_vec(IxDyn(&[2, 2]), vec![5.0f32; 4])
            .unwrap()
            .view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let result = file.read_array::<f32>("g", vec![], vec![]).await.unwrap();
    for row in 0..4usize {
        for col in 0..4usize {
            let val = result[[row, col]];
            if (1..3).contains(&row) && (1..3).contains(&col) {
                assert_eq!(val, 5.0, "row={row} col={col}");
            } else {
                assert_eq!(val, 0.0, "row={row} col={col}");
            }
        }
    }
}

#[tokio::test]
async fn sub_region_read_after_partial_update() {
    // [10] with chunk=[5]. Write 0..10, flush. Patch indices [3..7] with 99, flush.
    // Read first chunk [0..5]: expect [0,1,2,99,99].
    // Read second chunk [5..10]: expect [99,99,7,8,9].
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>("arr", vec!["x".into()], vec![10], Some(vec![5]), None)
        .unwrap();
    file.write_array(
        "arr",
        vec![0],
        Array::from_vec((0..10i32).collect::<Vec<_>>())
            .into_dyn()
            .view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    file.write_array(
        "arr",
        vec![3],
        Array::from_vec(vec![99i32; 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let first = file
        .read_array::<i32>("arr", vec![0], vec![5])
        .await
        .unwrap();
    assert_eq!(
        first.iter().cloned().collect::<Vec<_>>(),
        vec![0, 1, 2, 99, 99]
    );

    let second = file
        .read_array::<i32>("arr", vec![5], vec![5])
        .await
        .unwrap();
    assert_eq!(
        second.iter().cloned().collect::<Vec<_>>(),
        vec![99, 99, 7, 8, 9]
    );
}

// ── Statistics tests ──────────────────────────────────────────────────

#[tokio::test]
async fn stats_flush_computes_correct_min_max_null_row_count() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "data",
        vec!["x".into()],
        vec![6],
        None,
        Some(FillValue::Int(1)),
    )
    .unwrap();
    // values: [3, 1, 4, 1, 5, 9] — fill=1 → 2 nulls
    file.write_array(
        "data",
        vec![0],
        Array::from_vec(vec![3i32, 1, 4, 1, 5, 9]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let stats = file.array_stats("data").expect("stats missing after flush");
    assert_eq!(stats.min, Some(StatValue::Int(3))); // fill value 1 excluded from range
    assert_eq!(stats.max, Some(StatValue::Int(9)));
    assert_eq!(stats.null_count, 2);
    assert_eq!(stats.row_count, 6);
}

#[tokio::test]
async fn stats_second_chunk_aggregates_globally() {
    // Two flushes, each adding one chunk. Stats should cover both.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>("a", vec!["x".into()], vec![10], Some(vec![5]), None)
        .unwrap();

    // Chunk [0]: values 1..=5
    file.write_array(
        "a",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4, 5]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    // Chunk [1]: values 6..=10
    file.write_array(
        "a",
        vec![5],
        Array::from_vec(vec![6i32, 7, 8, 9, 10]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let stats = file.array_stats("a").expect("stats missing");
    assert_eq!(stats.min, Some(StatValue::Int(1)));
    assert_eq!(stats.max, Some(StatValue::Int(10)));
    assert_eq!(stats.row_count, 10);
    assert_eq!(stats.null_count, 0);
}

#[tokio::test]
async fn stats_update_after_chunk_overwrite() {
    // Write chunk, flush. Overwrite same chunk with higher values, flush.
    // Stats must reflect the new data only.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>("x", vec!["i".into()], vec![4], None, None)
        .unwrap();

    file.write_array(
        "x",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    // Overwrite with larger values
    file.write_array(
        "x",
        vec![0],
        Array::from_vec(vec![10i32, 20, 30, 40]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let stats = file.array_stats("x").expect("stats missing");
    assert_eq!(stats.min, Some(StatValue::Int(10)));
    assert_eq!(stats.max, Some(StatValue::Int(40)));
    assert_eq!(stats.row_count, 4);
}

#[tokio::test]
async fn stats_survive_compact() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>("v", vec!["i".into()], vec![4], None, None)
        .unwrap();
    file.write_array(
        "v",
        vec![0],
        Array::from_vec(vec![5i32, 3, 8, 1]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();
    file.compact().await.unwrap();

    let stats = file.array_stats("v").expect("stats missing after compact");
    assert_eq!(stats.min, Some(StatValue::Int(1)));
    assert_eq!(stats.max, Some(StatValue::Int(8)));
    assert_eq!(stats.row_count, 4);
}

#[tokio::test]
async fn stats_loaded_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let path = object_store::path::Path::from("data.af");

    {
        let mut file = ArrayFile::create(store.clone(), path.clone(), small_config())
            .await
            .unwrap();
        file.define_array::<i32>("nums", vec!["i".into()], vec![3], None, None)
            .unwrap();
        file.write_array(
            "nums",
            vec![0],
            Array::from_vec(vec![7i32, 2, 5]).into_dyn().view(),
        )
        .await
        .unwrap();
        file.flush().await.unwrap();
    }

    // Re-open and verify stats are loaded from .stats file.
    let file = ArrayFile::open(store.clone(), path.clone(), small_config())
        .await
        .unwrap();
    let stats = file.array_stats("nums").expect("stats not loaded on open");
    assert_eq!(stats.min, Some(StatValue::Int(2)));
    assert_eq!(stats.max, Some(StatValue::Int(7)));
    assert_eq!(stats.row_count, 3);
}

#[tokio::test]
async fn stats_none_before_first_flush() {
    let file = ArrayFile::create_memory(small_config()).await.unwrap();
    assert!(file.array_stats("anything").is_none());
}

#[tokio::test]
async fn stats_unwritten_chunks_count_as_nulls() {
    // Shape [10], chunk_shape [5] → 2 possible chunks.
    // Write only chunk [0]; chunk [1] is never written.
    // row_count must equal total shape product; unwritten elements count as nulls.
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>(
        "partial",
        vec!["x".into()],
        vec![10],
        Some(vec![5]),
        Some(FillValue::Int(0)),
    )
    .unwrap();

    // Write only the first chunk; leave the second unwritten.
    file.write_array(
        "partial",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3, 4, 5]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let stats = file.array_stats("partial").expect("stats missing");
    assert_eq!(stats.row_count, 10); // full array capacity
    assert_eq!(stats.null_count, 5); // 5 unwritten elements
    assert_eq!(stats.min, Some(StatValue::Int(1)));
    assert_eq!(stats.max, Some(StatValue::Int(5)));
}

#[tokio::test]
async fn timestamp_ns_roundtrip_and_stats() {
    let fill = 1_000_000_000i64; // 1 second past the epoch — our "missing" marker
    let values = vec![
        TimestampNs(0),
        TimestampNs(fill),
        TimestampNs(2_000_000_000),
        TimestampNs(-500),
        TimestampNs(fill),
    ];
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<TimestampNs>(
        "events",
        vec!["t".into()],
        vec![values.len()],
        None,
        Some(FillValue::TimestampNs(fill)),
    )
    .unwrap();
    file.write_array(
        "events",
        vec![0],
        Array::from_vec(values.clone()).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();

    let back = file
        .read_array::<TimestampNs>("events", vec![], vec![])
        .await
        .unwrap();
    assert_eq!(back.iter().cloned().collect::<Vec<_>>(), values);

    let stats = file.array_stats("events").expect("stats missing");
    assert_eq!(stats.min, Some(StatValue::TimestampNs(-500)));
    assert_eq!(stats.max, Some(StatValue::TimestampNs(2_000_000_000)));
    assert_eq!(stats.null_count, 2);
    assert_eq!(stats.row_count, values.len() as u64);
}

/// Deleting an array must drop its statistics, not leave them readable.
///
/// `mark_deleted` clears the chunk list, so a deleted array never re-enters the
/// dirty set — before `StatsFile::remove` existed, its stale min/max survived
/// every subsequent flush and only disappeared at the next `compact`.
#[tokio::test]
async fn delete_drops_array_stats() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    file.define_array::<i32>("gone", vec!["x".into()], vec![3], None, None)
        .unwrap();
    file.write_array(
        "gone",
        vec![0],
        Array::from_vec(vec![7i32, 8, 9]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.define_array::<i32>("kept", vec!["x".into()], vec![3], None, None)
        .unwrap();
    file.write_array(
        "kept",
        vec![0],
        Array::from_vec(vec![1i32, 2, 3]).into_dyn().view(),
    )
    .await
    .unwrap();
    file.flush().await.unwrap();
    assert!(file.array_stats("gone").is_some(), "precondition");

    file.delete("gone").unwrap();
    assert!(
        file.array_stats("gone").is_none(),
        "stats must go as soon as the array is deleted"
    );

    // ...and must not come back after the flush that persists the tombstone.
    file.flush().await.unwrap();
    assert!(file.array_stats("gone").is_none(), "stale stats resurrected");
    assert!(
        file.array_stats("kept").is_some(),
        "deleting one array must not disturb another"
    );
}

/// `entries()` is the bulk accessor: one pass over every entry, instead of an
/// O(n) `get_array` scan per name.
#[tokio::test]
async fn stats_entries_exposes_every_array() {
    let mut file = ArrayFile::create_memory(small_config()).await.unwrap();
    for name in ["a", "b", "c"] {
        file.define_array::<i32>(name, vec!["x".into()], vec![2], None, None)
            .unwrap();
        file.write_array(
            name,
            vec![0],
            Array::from_vec(vec![1i32, 2]).into_dyn().view(),
        )
        .await
        .unwrap();
    }
    file.flush().await.unwrap();

    let stats = file.stats().expect("stats after flush");
    let mut names: Vec<&str> = stats.entries().iter().map(|s| s.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["a", "b", "c"]);
}
