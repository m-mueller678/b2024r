use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::addr_of_mut;

fn main() {
    println!("Hello, world!");
}

unsafe trait SeqLockable{
    type AsType;
}

#[repr(transparent)]
pub struct SeqLockGuardedExclusive<'a,T>{
    p:*mut T,
    _p:PhantomData<&'a mut T>,
}

#[repr(transparent)]
pub struct SeqLockGuardedOptimistic<'a,T>{
    p:*const T,
    _p:PhantomData<&'a T>,
}

macro_rules! seqlock_accessors {
    (struct $This:ty: $($vis:vis $name:ident : $T:ty),*) => {
        impl<'a> SeqLockGuardedExclusive<'a,$This>{
            $($vis fn $name<'b>(&'b mut self)->SeqLockGuardedExclusive<'b,$T>{
                unsafe{
                    SeqLockGuardedExclusive{p:std::ptr::addr_of_mut!((*self.p).$name),_p:PhantomData}
                }
            })*
        }

        impl<'a> SeqLockGuardedOptimistic<'a,$This>{
            $($vis fn $name<'b>(&'b self)->SeqLockGuardedOptimistic<'b,$T>{
                unsafe{
                    SeqLockGuardedOptimistic{p:std::ptr::addr_of!((*self.p).$name),_p:PhantomData}
                }
            })*
        }
    };
}

macro_rules! seqlock_primitive {
    ($($T:ty),*) => {
        $(
        impl SeqLockGuardedOptimistic<'_,$T>{
            pub fn load(&self)->$T{
                todo!()
            }
        }

        impl SeqLockGuardedExclusive<'_,$T>{
            pub fn store(&mut self,v:$T){
                unsafe{self.p.write(v);}
            }
            pub fn load(&self)->$T{
                unsafe{self.p.read()}
            }
        }
        )*
    };
}

struct MyStruct{
    a:u32,
    b:i64,
}

seqlock_accessors!(struct MyStruct: a:u32,b:i64);
seqlock_primitive!(u8,u16,u32,u64);