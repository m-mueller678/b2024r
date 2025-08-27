
use umolc::{BufferManager};
use umolc_btree::{Page, Tree};
pub fn check_node_tag_percentage<'bm, BM>(
    node_tag: u8,
    expected_percentage: f32,
    action: &str,
    allow_good_heads: bool,
    greater_than: bool,
    tree: &Tree<'bm, BM>,
) where
    BM: BufferManager<'bm, Page = Page>,
{
    let mut total = 0.0f32;
    let mut correct = 0.0f32;

    tree.scan_node_types(b"\0", |x, scan_counter|{


        if !allow_good_heads && scan_counter == 255 {
            panic!("Node should not have good heads!");
        }
        if x == node_tag {
            correct += 1.0;
        }
        total += 1.0;


        false
    });

    let margin = correct / total;

    if (margin >= expected_percentage && greater_than) || (margin <= expected_percentage && !greater_than) {
        println!("After spamming {:<8}, {:>3.0}% of nodes have tag {node_tag} (required {expected_percentage:.2}% of {} nodes)", action, margin * 100.0, total as usize);
    }
    else {
        panic!("Error: After spamming {:<8}, {:>3.0}% of nodes have tag {node_tag} (required {expected_percentage:.2}% of {} nodes)", action, margin * 100.0, total as usize);
    }
}

