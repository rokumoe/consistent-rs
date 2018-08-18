extern crate md5;

use std::cmp::Ordering;
use std::rc::Rc;

mod hash {
    use std::num::Wrapping;

    pub fn bkdr(s: &[u8]) -> u32 {
        const SEED: u32 = 313131;
        let mut h = Wrapping(0);
        for b in s.iter() {
            h = h * Wrapping(SEED) + Wrapping(*b as u32);
        }
        return h.0;
    }
}

#[derive(Debug, Eq)]
struct Vnode {
    hash: u32,
    val: Rc<String>,
}

impl Ord for Vnode {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.hash < other.hash {
            Ordering::Less
        } else if self.hash > other.hash {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl PartialOrd for Vnode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Vnode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

pub struct Ketama {
    nodes: Vec<Vnode>,
}

impl Ketama {
    pub fn new(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, val: &str, spot: usize) {
        const PER_HASH: usize = 4;
        let n = (spot + PER_HASH - 1) / PER_HASH;
        let mut i = 0;
        let val = Rc::new(val.to_owned());
        while i < n {
            let sum = md5::compute(format!("{}+{}", val, i).as_bytes());
            for j in 0..PER_HASH {
                let hash = unsafe { *((&sum[j * 4] as *const u8) as *const u32) };
                self.nodes.push(Vnode {
                    hash,
                    val: val.clone(),
                });
            }
            i += 1;
        }
    }

    pub fn build(&mut self) {
        self.nodes.sort();
    }

    pub fn query_u32(&self, key: u32) -> Option<&str> {
        if self.nodes.is_empty() {
            return None;
        }
        let idx = self.nodes.binary_search_by(|probe| {
            if probe.hash < key {
                Ordering::Less
            } else if probe.hash > key {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        });
        let idx = match idx {
            Ok(idx) => idx,
            Err(idx) => if idx < self.nodes.len() {
                idx
            } else {
                0
            },
        };
        let node = &self.nodes[idx];
        Some(node.val.as_str())
    }

    pub fn query(&self, key: &str) -> Option<&str> {
        self.query_u32(hash::bkdr(key.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_init(ring: &mut Ketama) {
        ring.add("a", 128);
        ring.add("b", 128);
        let strc = String::from("c");
        ring.add(strc.as_str(), 128);
        ring.build();
    }

    #[test]
    fn test_query() {
        let mut ring = Ketama::new(128 * 3);
        ring_init(&mut ring);
        println!("{:?}", ring.query("ok"));
        println!("{:?}", ring.query("233"));
        println!("{:?}", ring.query("666"));
        {
            let a = ring.query("a");
            println!("{:?}", a);
        }
        ring.nodes.clear();
    }
}
