
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

    tree.scan_node_types(b"\0", |x, scan_counter, _|{


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

pub fn average_leaf_count<'bm, BM>(
    tree: &Tree<'bm, BM>,
) -> u16 where
    BM: BufferManager<'bm, Page = Page>,
{
    let mut nodes = 0;
    let mut total = 0;

    tree.scan_node_types(b"\0", |_, _, count|{


        nodes += 1;
        total += count;
        false
    });

    total / nodes
}


pub fn total_leaf_count<'bm, BM>(
    tree: &Tree<'bm, BM>,
) -> u16 where
    BM: BufferManager<'bm, Page = Page>,
{
    let mut nodes = 0;

    tree.scan_node_types(b"\0", |_, _, _|{


        nodes += 1;
        false
    });

    nodes
}
pub fn amount_values<'bm, BM>(
    tree: &Tree<'bm, BM>,
) -> usize where
    BM: BufferManager<'bm, Page = Page>,
{
    let mut values = 0;

    tree.scan(b"\0", |_, _|{


        values += 1;
        false
    });

    values
}
