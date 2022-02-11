use libarchive3_sys::ffi::{
    archive_entry_clear, archive_entry_pathname, archive_entry_size,
    archive_read_data, archive_read_free, archive_read_new, archive_read_next_header,
    archive_read_open_memory, archive_read_support_filter_all, archive_read_support_format_all, //archive_entry_free
};
use libarchive3_sys::ffi::{Struct_archive, Struct_archive_entry};
use libarchive3_sys::ffi::{ARCHIVE_EOF, ARCHIVE_OK, ARCHIVE_WARN};
use std::ffi::{c_void, CStr};

use crate::MAX_FILESIZE;

pub struct ArchiveManager {
    arch: *mut Struct_archive,
    entry: *mut Struct_archive_entry,
    last_status: i32,
}

impl ArchiveManager {
    pub fn new() -> ArchiveManager {
        ArchiveManager {
            arch: 0 as *mut Struct_archive,
            entry: 0 as *mut Struct_archive_entry,
            last_status: 0,
        }
    }
    pub fn set_archive(&mut self, arch_data: &mut Vec<u8>) -> Result<(), ()> {
        unsafe {
            self.arch = archive_read_new();
            archive_read_support_filter_all(self.arch);
            archive_read_support_format_all(self.arch);

            if archive_read_open_memory(
                self.arch,
                &mut arch_data[0] as *mut u8 as *mut c_void,
                arch_data.len(),
            ) != ARCHIVE_OK
            {
                return Err(());
            }
        }
        Ok(())
    }

    pub fn entry_name(&mut self) -> Result<String, ()> {
        if self.entry != 0 as *mut Struct_archive_entry
            && (self.last_status == ARCHIVE_OK || self.last_status == ARCHIVE_WARN)
        {
            unsafe {
                match CStr::from_ptr(archive_entry_pathname(self.entry)).to_str() {
                    Ok(cstr) => return Ok(String::from(cstr)),
                    Err(_) => return Err(()),
                }
            }
        }
        return Err(());
    }

    pub fn prepare_next(&mut self) -> Result<bool, String> {
        unsafe {
            self.last_status = archive_read_next_header(self.arch, &mut self.entry);
            // skip if size is too big or extension is unsupported
            while self.last_status == ARCHIVE_OK
                && (archive_entry_size(self.entry) > MAX_FILESIZE as i64
                    || !self.entry_name().unwrap().ends_with(".txt"))
            {
                // archive_entry_free(self.entry);
                self.last_status = archive_read_next_header(self.arch, &mut self.entry);
            }

            if self.last_status == ARCHIVE_EOF {
                if archive_read_free(self.arch) != ARCHIVE_OK {
                    return Err(String::from("Could not close the archive"));
                }
                return Ok(false);
            }

            if self.last_status == ARCHIVE_WARN || self.last_status == ARCHIVE_OK {
                return Ok(true);
            }

            if archive_read_free(self.arch) != ARCHIVE_OK {
                return Err(String::from("Could not close the archive"));
            }
            return Err(String::from("Could not read entry in archive"));
        }
    }

    pub fn get_next(&mut self) -> Result<Vec<u8>, String> {
        let mut result: Vec<u8> = Vec::new();
        if self.last_status != ARCHIVE_OK && self.last_status != ARCHIVE_WARN {
            return Err(String::from("Archive EOF"));
        }

        unsafe {
            let entry_size = archive_entry_size(self.entry) as usize;
            result.reserve(entry_size);
            result.set_len(entry_size);
            archive_read_data(
                self.arch,
                &mut result[0] as *mut u8 as *mut c_void,
                entry_size,
            );

            archive_entry_clear(self.entry);
        }
        Ok(result)
    }
}

// impl Drop for ArchiveManager {
//     fn drop(&mut self) {
//         unsafe {
//             // archive_entry_free(self.entry);
//         }
//     }
// }
