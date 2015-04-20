use core::fmt::Debug;
use std::any::Any;
use std::mem;

use progress::Timestamp;
use progress::count_map::CountMap;

use communication::Observer;

use std::rc::Rc;
use std::cell::RefCell;

pub trait Data : Clone+Send+Debug+Any { }
impl<T: Clone+Send+Debug+Any> Data for T { }

pub struct OutputPort<T: Timestamp, D: Data> {
    limit:  usize,
    buffer: Vec<D>,
    shared: Rc<RefCell<Vec<Box<Observer<Time=T,Data=Vec<D>>>>>>,
    stash:  Rc<RefCell<Vec<Vec<D>>>>,
}

impl<T: Timestamp, D: Data> Observer for OutputPort<T, D> {
    type Time = T;
    type Data = D;
    #[inline(always)] fn open(&mut self, time: &T) {
        for observer in self.shared.borrow_mut().iter_mut() { observer.open(time); }
    }
    #[inline(always)] fn show(&mut self, data: &D) { self.give(data.clone()); }
    #[inline(always)] fn give(&mut self, data:  D) {
        self.buffer.push(data);
        if self.buffer.len() > self.limit { self.flush_buffer(); }
    }
    #[inline(always)] fn shut(&mut self, time: &T) {
        if self.buffer.len() > 0 { self.flush_buffer(); }
        for observer in self.shared.borrow_mut().iter_mut() { observer.shut(time); }
    }
}

impl<T: Timestamp, D: Data> OutputPort<T, D> {
    pub fn new() -> OutputPort<T, D> {
        let limit = 256;    // TODO : Used to be a parameter, but not clear that the user should
                            // TODO : need to know the right value here. Think a bit harder...
        OutputPort {
            limit:  limit,
            buffer: Vec::with_capacity(limit),
            shared: Rc::new(RefCell::new(Vec::new())),
            stash:  Rc::new(RefCell::new(Vec::new())),
        }
    }
    pub fn add_observer<O: Observer<Time=T, Data=D>+'static>(&self, observer: O) {
        self.shared.borrow_mut().push(Box::new(OutputPortFlattener::new(observer, self.stash.clone())));
    }

    fn flush_buffer(&mut self) {
        let mut observers = self.shared.borrow_mut();
        for index in (0..observers.len()) {
            let data = mem::replace(&mut self.buffer, self.stash.borrow_mut().pop().unwrap_or(Vec::new()));
            if index < observers.len() - 1 { println!("cloning data"); self.buffer.push_all(&data); }
            observers[index].give(data);
        }
        self.buffer.clear(); // in case observers.len() == 0
    }
}

// TODO : un-implement, and have 'new' return a pair of pub/sub types
impl<T: Timestamp, D: Data> Clone for OutputPort<T, D> {
    fn clone(&self) -> OutputPort<T, D> {
        OutputPort {
            limit:  self.limit,
            buffer: Vec::new(),
            shared: self.shared.clone(),
            stash:  self.stash.clone(),
        }
    }
}


// dual to BufferedObserver, flattens out buffers
pub struct OutputPortFlattener<O: Observer> {
    observer:   O,
    stash:      Rc<RefCell<Vec<Vec<O::Data>>>>,
}

impl<O: Observer> OutputPortFlattener<O> {
    pub fn new(observer: O, stash: Rc<RefCell<Vec<Vec<O::Data>>>>) -> OutputPortFlattener<O> {
        OutputPortFlattener {
            observer: observer,
            stash:    stash,
        }
    }
}

impl<O: Observer> Observer for OutputPortFlattener<O> {
    type Time = O::Time;
    type Data = Vec<O::Data>;
    #[inline(always)] fn open(&mut self, time: &O::Time) -> () { self.observer.open(time); }
    #[inline(always)] fn show(&mut self, _data: &Vec<O::Data>) -> () {
        for datum in _data { self.observer.show(datum); }
    }
    #[inline(always)] fn give(&mut self, mut data:  Vec<O::Data>) -> () {
        for datum in data.drain() { self.observer.give(datum); }
        self.stash.borrow_mut().push(data);
    }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () { self.observer.shut(time); }
}


pub struct ObserverHelper<O: Observer> {
    observer:   O,
    counts:     Rc<RefCell<CountMap<O::Time>>>,
    count:      i64,
}

impl<O: Observer> Observer for ObserverHelper<O> where O::Time : Timestamp {
    type Time = O::Time;
    type Data = O::Data;
    #[inline(always)] fn open(&mut self, time: &O::Time) { self.observer.open(time); }
    #[inline(always)] fn show(&mut self, data: &O::Data) { self.count += 1; self.observer.show(data); }
    #[inline(always)] fn give(&mut self, data:  O::Data) { self.count += 1; self.observer.give(data); }
    #[inline(always)] fn shut(&mut self, time: &O::Time) -> () {
        self.counts.borrow_mut().update(time, self.count);
        self.observer.shut(time);
        self.count = 0;
    }
}

impl<O: Observer> ObserverHelper<O> where O::Time : Eq+Clone+'static {
    pub fn new(observer: O, counts: Rc<RefCell<CountMap<O::Time>>>) -> ObserverHelper<O> {
        ObserverHelper {
            observer:   observer,
            counts:     counts,
            count:      0,
        }
    }

    #[inline(always)] pub fn pull_progress(&mut self, updates: &mut CountMap<O::Time>) {
        while let Some((ref time, delta)) = self.counts.borrow_mut().pop() { updates.update(time, delta); }
    }
}
