// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub trait Shim {
    fn unwrap(self) -> Self;
}

impl<'a, T: parking_lot::lock_api::RawRwLock, U> Shim
for parking_lot::lock_api::RwLockReadGuard<'a, T, U>
{
    fn unwrap(self) -> Self {
        self
    }
}

impl<'a, T: parking_lot::lock_api::RawRwLock, U> Shim
for parking_lot::lock_api::RwLockWriteGuard<'a, T, U>
{
    fn unwrap(self) -> Self {
        self
    }
}

impl<'a, T: parking_lot::lock_api::RawMutex, U> Shim
for parking_lot::lock_api::MutexGuard<'a, T, U>
{
    fn unwrap(self) -> Self {
        self
    }
}

#[cfg(test)]
mod test {
    use crate::lock::Shim;

    #[test]
    fn test_parking_lot_lock() {
        let lock = parking_lot::Mutex::new(());
        let _x = lock.lock().unwrap();
        drop(_x);
    }
}