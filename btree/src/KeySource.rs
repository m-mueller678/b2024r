use seqlock::{Exclusive, Guarded};

trait KeySource{
    fn write_to(&self,dst:Guarded<Exclusive,[u8]>);
}