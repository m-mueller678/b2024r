use std::cell::RefCell;
use crate::basic_node::{BasicInner, BasicLeaf, BasicNode};
use crate::key_source::SourceSlice;
use crate::node::{node_guard_cast, node_tag, CommonNodeHead, KindInner, Node, ParentInserter};
use crate::page::{page_id_to_3x16, PageId, PageTail, PAGE_TAIL_SIZE};
use crate::{MAX_KEY_SIZE, W};
use bytemuck::{Pod, Zeroable};
use seqlock::{BmExt, BufferManager, Exclusive, Guard, Guarded, Optimistic, SeqlockAccessors};
use std::marker::PhantomData;
use std::mem::size_of;
use seqlock::unwind::catch;

pub const FENCES_INCLUDE_PREFIX:bool=true;
pub const BRANCH_HIGH_ON_EQUAL:bool=true;

pub struct Tree<'bm, BM: BufferManager<'bm, Page = PageTail>> {
    meta: u64,
    bm: BM,
    _p: PhantomData<&'bm BM>,
}

impl<'bm, BM: BufferManager<'bm, Page = PageTail>> Tree<'bm, BM> {
    pub fn new(bm: BM) -> Self {
        let (meta_id, mut meta_guard) = bm.lock_new();
        let (root_id, mut root_guard) = bm.lock_new();
        {
            let mut meta = meta_guard.b().0.cast::<MetadataPage>();
            meta.b().root_mut().store(root_id);
            meta.node_head_mut().tag_mut().store(node_tag::METADATA_MARKER);
        }
        root_guard.b().0.cast::<BasicLeaf>().init(&[][..], &[][..], [0u8; 3]);
        Tree { meta: meta_id, bm, _p: PhantomData }
    }

    fn validate_fences_exclusive(&self) {
        let mut low_buffer = [0u8; MAX_KEY_SIZE];
        let mut high_buffer = [0u8; MAX_KEY_SIZE];
        let meta = self.bm.lock_exclusive(self.meta);
        let mut root = self.bm.lock_exclusive(meta.s().cast::<MetadataPage>().root().load());
        drop(meta);
        root.b().0.cast::<BasicInner>().validate_inter_node_fences(
            self.bm,
            &mut &mut low_buffer,
            &mut &mut high_buffer,
            0,
            0,
        );
    }

    pub fn remove(&self, k: &[u8]) -> Option<()> {
        let mut removed = false;
        seqlock::unwind::repeat(|| {
            self.try_remove(k, &mut removed);
        });
        if removed {
            Some(())
        } else {
            None
        }
    }

    fn try_remove(&self, k: &[u8], removed: &mut bool) {
        let [parent, node] = self.descend(k, None);
        let mut node = node.upgrade();
        if node.b().node_cast::<BasicLeaf>().remove(k).is_some() {
            *removed = true;
        }
        parent.release_unchecked();
        //TODO merge nodes
    }

    pub fn insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let x = seqlock::unwind::repeat(|| self.try_insert(k, val));
        self.validate_fences_exclusive();
        x
    }

    fn descend(&self, k: &[u8], stop_at: Option<PageId>) -> [Guard<'bm, BM, Optimistic, PageTail>; 2] {
        assert!(BRANCH_HIGH_ON_EQUAL);
        let mut parent = self.bm.lock_optimistic(self.meta);
        let mut node_pid = parent.s().cast::<MetadataPage>().root().load();
        let mut node = self.bm.lock_optimistic(node_pid);
        parent.check();
        while node.s().common().tag().load() == node_tag::BASIC_INNER && Some(node_pid) != stop_at {
            parent.release_unchecked();
            parent = node;
            node_pid = parent.node_cast::<BasicInner>().lookup_inner(k, true);
            node = self.bm.lock_optimistic(node_pid);
            parent.check();
        }
        [parent, node]
    }

    fn split_and_insert(&self, split_target: PageId, k: &[u8], val: &[u8]) -> Option<()> {
        let parent_id = {
            let [parent, node] = self.descend(k, Some(split_target));
            if self.bm.page_id(node.page_address()) == split_target {
                let mut node = node.upgrade();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut parent);
                debug_assert!(node.common().tag().load() == node_tag::BASIC_INNER);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicInner>(),
                    parent.b().node_cast::<BasicInner>(),
                    self.bm,
                )
                .is_ok()
                {
                    None
                } else {
                    Some(self.bm.page_id(parent.page_address()))
                }
            } else {
                None
            }
        };
        if let Some(p) = parent_id {
            self.split_and_insert(p, k, val)
        } else {
            self.try_insert(k, val)
        }
    }

    fn ensure_parent_not_root(&self, parent: &mut Guard<'bm, BM, Exclusive, PageTail>) {
        if self.bm.page_id(parent.page_address()) == self.meta {
            let mut meta = parent.b().0.cast::<MetadataPage>();
            let (new_root_id, mut new_root_guard) = self.bm.lock_new();
            new_root_guard.b().0.cast::<BasicInner>().init(&[][..], &[][..], page_id_to_3x16(meta.root().load()));
            meta.root_mut().store(new_root_id);
            *parent = new_root_guard
        }
    }

    fn try_insert(&self, k: &[u8], val: &[u8]) -> Option<()> {
        let [parent, node] = self.descend(k, None);
        let mut node = node.upgrade();
        match node.b().node_cast::<BasicLeaf>().insert_leaf(k, val) {
            Ok(x) => {
                parent.release_unchecked();
                x
            }
            Err(()) => {
                node.reset_written();
                let mut parent = parent.upgrade();
                self.ensure_parent_not_root(&mut parent);
                if Self::split_locked_node(
                    k,
                    &mut node.b().node_cast::<BasicLeaf>(),
                    parent.b().node_cast::<BasicInner>(),
                    self.bm,
                )
                .is_err()
                {
                    let parent_id = self.bm.page_id(parent.page_address());
                    drop(parent);
                    drop(node);
                    return self.split_and_insert(parent_id, k, val);
                }
                drop(parent);
                drop(node);
                // TODO could descend from parent
                self.try_insert(k, val)
            }
        }
    }

    pub fn range_iterator<'t,'b>(&'t self, mut start: &[u8],buffer:&mut[u8;MAX_KEY_SIZE]) -> TreeIterator<'b,'t, 'bm, BM> {
        let mut iter =
            TreeIterator { key_buffer: buffer, tree: self, current_leaf: Err((false, start.len())) };
        iter
    }

    pub fn lookup_to_vec(&self, k: &[u8]) -> Option<Vec<u8>> {
        seqlock::unwind::repeat(|| self.try_lookup(k).map(|v| v.load_slice_to_vec()))
    }

    pub fn lookup_inspect<R>(&self, k: &[u8], mut f: impl FnMut(Option<Guard<'bm, BM, Optimistic, [u8]>>) -> R) -> R {
        seqlock::unwind::repeat(move || f(self.try_lookup(k)))
    }

    pub fn try_lookup(&self, k: &[u8]) -> Option<Guard<'bm, BM, Optimistic, [u8]>> {
        let [parent, node] = self.descend(k, None);
        drop(parent);
        let key: Guarded<'bm, _, _> = node.node_cast::<BasicLeaf>().lookup_leaf(k)?;
        Some(unsafe { node.map(|_| key) })
    }

    fn split_locked_node<N: Node>(
        k: &[u8],
        leaf: &mut W<Guarded<'_, Exclusive, N>>,
        parent: W<Guarded<Exclusive, BasicNode<KindInner>>>,
        bm: BM,
    ) -> Result<(), ()> {
        N::split(leaf, (parent, bm), k)
    }

    pub fn lock_path(&self, key: &[u8]) -> Vec<Guard<'bm, BM, Exclusive, PageTail>> {
        let mut path = Vec::new();
        let mut node = {
            let parent = self.bm.lock_exclusive(self.meta);
            let node_pid = parent.s().cast::<MetadataPage>().root().load();
            path.push(parent);
            self.bm.lock_exclusive(node_pid)
        };
        while node.s().common().tag().load() == node_tag::BASIC_INNER {
            let node_pid = node.s().node_cast::<BasicInner>().optimistic().lookup_inner(key, true);
            path.push(node);
            node = self.bm.lock_exclusive(node_pid);
        }
        path.push(node);
        path
    }
}

impl<'bm, BM: BufferManager<'bm, Page = PageTail>> Drop for Tree<'bm, BM> {
    fn drop(&mut self) {
        fn free_recursive<'bm, BM: BufferManager<'bm, Page = PageTail>>(bm: BM, p: PageId) {
            let node = bm.lock_exclusive(p);
            if node.tag().load() == BasicInner::TAG {
                let node = node.optimistic().node_cast::<BasicInner>();
                for i in 0..node.count().load() as usize {
                    free_recursive(bm, node.index_child(i))
                }
            }
            node.free()
        }
        let mut meta_lock = self.bm.lock_exclusive(self.meta);
        free_recursive(self.bm, meta_lock.b().0.cast::<MetadataPage>().root().load());
        meta_lock.free();
    }
}

#[derive(SeqlockAccessors, Pod, Zeroable, Copy, Clone)]
#[repr(C)]
#[seq_lock_wrapper(crate::W)]
struct MetadataPage {
    // for debugging
    node_head: CommonNodeHead,
    #[seq_lock_skip_accessor]
    _pad: [u8; PAGE_TAIL_SIZE - 8 - size_of::<CommonNodeHead>()],
    root: PageId,
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub enum Supreme<T> {
    X(T),
    Sup,
}

impl<'g, 'bm, BM: BufferManager<'bm, Page = PageTail>> ParentInserter<'bm, BM>
    for (W<Guarded<'g, Exclusive, BasicNode<KindInner>>>, BM)
{
    fn insert_upper_sibling(self, separator: impl SourceSlice) -> Result<Guard<'bm, BM, Exclusive, PageTail>, ()> {
        let (mut guard, bm) = self;
        let (new_page, new_guard) = bm.lock_new();
        separator.to_stack_buffer::<MAX_KEY_SIZE, _>(|sep| {
            if let Ok(()) = guard.insert_inner(sep, new_page) {
                Ok(new_guard)
            } else {
                new_guard.free();
                Err(())
            }
        })
    }
}

pub struct TreeIterator<'b, 't, 'bm, BM: BufferManager<'bm, Page = PageTail>> {
    key_buffer: [u8; MAX_KEY_SIZE],
    key_len:usize,
    prefix_len:usize,
    is_known_first_key:bool,
    assume_key_first:bool,
    use_lock_coupling:bool,
    tree: &'t Tree<'bm, BM>,
    current_leaf:Option< (Guard<'bm, BM, Optimistic, BasicLeaf>, isize) >,
}

enum TreeIteratorResult<'iter,'bm,BM: BufferManager<'bm, Page = PageTail>>{
    KeyValue{
        key:&'iter[u8],
        value:Guarded<'iter,Optimistic,[u8]>
    },
    PageEnd{
        fence_key:&'iter [u8],
        lock:Guard<'bm,BM,Optimistic,[u8]>,
    },
    LockFail{
        last_key:&'iter [u8],
    }
}

impl<'b, 't, 'bm, BM: BufferManager<'bm, Page = PageTail>> TreeIterator<'b, 't,'bm,BM> {
    fn try_tree_next<'iter>(&'iter mut self)->TreeIteratorResult<'iter,'bm,BM>{
        catch(||{
            self.tree_next()
        }).unwrap_or_else(|_|{
            TreeIteratorResult::LockFail {last_key:&self.key_buffer[..self.key_len]}
        })
    }

    fn tree_next<'iter>(&'iter mut self)->TreeIteratorResult<'iter,'bm,BM>{
        // be careful about unwind safety
        loop {
            return match &mut self.current_leaf {
                Some((node, index)) => {
                    if node.iterator_valid(*index) {
                        let suffix=node.iterator_suffix(*index);
                        let value = node.iterator_value(*index);
                        node.iterator_advance(index);
                        suffix.load_slice_into(&mut self.key_buffer[self.prefix_len..]);
                        self.key_len = self.prefix_len+suffix.len();
                        TreeIteratorResult::KeyValue {key:&self.key_buffer[..self.key_len],value}
                    } else {
                        if self.use_lock_coupling{
                            unimplemented!()
                        }else{
                            assert!(FENCES_INCLUDE_PREFIX);
                            assert!(BRANCH_HIGH_ON_EQUAL); // if fence key is present as key, it will be in next node
                            let fence=node.upper_fence().slice(self.prefix_len..);
                            self.key_len = self.prefix_len+fence.len();
                            self.is_known_first_key=self.assume_key_first;
                            fence.load_slice_into(&mut self.key_buffer[self.prefix_len..]);
                            let lock = self.current_leaf.take().0;
                            TreeIteratorResult::PageEnd {fence_key:&self.key_buffer[..self.key_len],lock}
                        }
                    }
                }
                None =>{
                    let [parent, node] = self.tree.descend(&self.key_buffer[..*self.key_len], None);
                    parent.release_unchecked();
                    let index = if self.is_known_first_key { 0 } else {
                        node.iterator_start(&self.key_buffer[..*self.key_len])
                    };
                    self.current_leaf = Some((node,index));
                    continue
                }
            }
        }
    }

    fn restart_with_last_key(&mut self){
        let () = self.current_leaf.take();
        self.current_leaf=None;
        self.assume_key_first=false;
    }
}

impl<'b,'a, 'bm, BM: BufferManager<'bm, Page = PageTail>> Iterator for TreeIterator<'b,'a, 'bm, BM> {
    type Item = (&'b [u8], Guarded<'bm, Optimistic, [u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.current_leaf {
                Ok((node, index)) => {
                    if node.iterator_valid(*index) {
                        let ret = (node.prefix(), node.iterator_suffix(*index), node.iterator_value(*index));
                        node.iterator_advance(index);
                        return Some(ret);
                    } else {
                        let prefix = node.prefix_len().load() as usize;
                        let fence = node.upper_fence().slice(prefix..);
                        fence.load_slice(&mut self.key_buffer[prefix..]);
                        self.current_leaf = Err((true, prefix + fence.len()));
                    }
                }
                Err((is_first, key_length)) => {
                    let [parent, node] = self.tree.descend(&self.key_buffer[..*key_length], None);
                    parent.release_unchecked();
                    let node = node_guard_cast::<BasicLeaf, _, _>(node);
                    let index = if *is_first { 0 } else { node.iterator_start(&self.key_buffer[..*key_length]) };
                    self.current_leaf = Ok((node, index))
                }
            }
        }
    }
}
