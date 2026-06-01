//! Writing and reading an on-disk file with Lz4 compression.
//!
//! ```sh
//! cargo run --example 07_on_disk
//! ```

use std::sync::Arc;

use array_format::{ArrayFile, FileConfig, Lz4Codec};
use ndarray::Array;
use object_store::local::LocalFileSystem;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap())
        as Arc<dyn object_store::ObjectStore>;
    let path = object_store::path::Path::from("data.af");

    // Create and write
    {
        let mut file =
            ArrayFile::create(Arc::clone(&store), path.clone(), FileConfig::new(Lz4Codec))
                .await
                .unwrap();

        file.define_array::<f32>(
            "matrix",
            vec!["r".into(), "c".into()],
            vec![4, 4],
            None,
            None,
        )
        .unwrap();
        let data: Vec<f32> = (0..16).map(|x| x as f32 * 0.5).collect();
        let nd = Array::from_shape_vec(ndarray::IxDyn(&[4, 4]), data).unwrap();
        file.write_array("matrix", vec![0, 0], nd.view())
            .await
            .unwrap();
        file.flush().await.unwrap();

        println!("wrote {} array(s)", file.list_arrays().len());
    }

    // Re-open and read
    {
        let file = ArrayFile::open(Arc::clone(&store), path, FileConfig::new(Lz4Codec))
            .await
            .unwrap();
        let out = file
            .read_array::<f32>("matrix", vec![], vec![])
            .await
            .unwrap();
        println!("matrix[3, 2] = {}", out[[3, 2]]); // 3*4 + 2 = 14 → 7.0
        assert!((out[[3, 2]] - 7.0).abs() < 1e-6);
    }
}
