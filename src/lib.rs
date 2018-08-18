extern crate md5;

use std::cmp::Ordering;

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

#[derive(Debug)]
struct Vnode<T> {
    hash: u32,
    val: T,
}

impl<T> Ord for Vnode<T> {
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

impl<T> Eq for Vnode<T> {}

impl<T> PartialOrd for Vnode<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Vnode<T> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

pub struct Ketama<T> {
    nodes: Vec<Vnode<T>>,
}

impl<T: Clone> Ketama<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, key: &str, val: T, spot: usize) {
        const PER_HASH: usize = 4;
        let n = (spot + PER_HASH - 1) / PER_HASH;
        let prefix = String::from(key) + "-";
        let mut i = 0;
        while i < n {
            let sum = md5::compute((prefix.clone() + i.to_string().as_str()).as_bytes());
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

    pub fn query_u32(&self, key: u32) -> Option<T> {
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
        Some(node.val.clone())
    }

    pub fn query(&self, key: &str) -> Option<T> {
        self.query_u32(hash::bkdr(key.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring_init(ring: &mut Ketama<i32>) {
        ring.add("a", 1, 128);
        ring.add("b", 2, 128);
        ring.add("c", 3, 128);
        ring.build();
    }

    #[test]
    fn test_query() {
        let mut ring = Ketama::new(128 * 3);
        ring_init(&mut ring);
        println!("nodes {:?}", ring.nodes);
        println!("query 233: {:?}", ring.query("233"));
        ring.nodes.clear();
    }
}
