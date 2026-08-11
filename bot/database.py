@@
         self._last_upload = 0
         self._upload_lock = threading.Lock()
         self._upload_enabled = False
         self._shutdown = False
+        # Backup/upload settings
+        self._backup_interval = 10  # seconds — configurable
+        self._backup_dir = os.path.join(os.path.dirname(self.db_path) or '.', 'db_backups')
+        os.makedirs(self._backup_dir, exist_ok=True)
+        self._local_timestamp_file = f"{self.db_path}.timestamp"
+        self._remote_timestamp = None
@@
-        if self.dropbox_client:
+        if self.dropbox_client:
@@
-            # Get remote file metadata
-            remote_timestamp = 0
-            try:
-                meta = self.dropbox_client.files_get_metadata(f"/{os.path.basename(self.db_path)}")
-                remote_timestamp = meta.server_modified.timestamp()
-            except ApiError as e:
-                if e.error.is_path() and e.error.get_path().is_not_found():
-                    remote_timestamp = 0
-                else:
-                    logger.error(f"Error getting remote metadata: {e}")
-                    remote_timestamp = 0
+            # Get remote file metadata (if exists)
+            remote_timestamp = 0
+            try:
+                meta = self.dropbox_client.files_get_metadata(f"/{os.path.basename(self.db_path)}")
+                # server_modified may be None for some file types; guard it
+                if getattr(meta, 'server_modified', None):
+                    remote_timestamp = meta.server_modified.timestamp()
+                else:
+                    remote_timestamp = 0
+                self._remote_timestamp = remote_timestamp
+            except ApiError as e:
+                if e.error.is_path() and e.error.get_path().is_not_found():
+                    remote_timestamp = 0
+                else:
+                    logger.error(f"Error getting remote metadata: {e}")
+                    remote_timestamp = 0
@@
-            # Get local file mtime (if exists)
-            local_timestamp = 0
-            if os.path.exists(self.db_path):
-                local_timestamp = os.path.getmtime(self.db_path)
+            # Get local file mtime (if exists)
+            local_timestamp = 0
+            if os.path.exists(self.db_path):
+                local_timestamp = os.path.getmtime(self.db_path)
@@
-            # Download if remote is newer or local doesn't exist
-            if not os.path.exists(self.db_path) or remote_timestamp > local_timestamp:
-                logger.info(f"Remote is newer ({remote_timestamp} > {local_timestamp}); downloading...")
-                # Backup existing local file (optional)
-                if os.path.exists(self.db_path):
-                    backup_path = f"{self.db_path}.backup"
-                    try:
-                        os.rename(self.db_path, backup_path)
-                        logger.info(f"Local database backed up to {backup_path}")
-                    except Exception as e:
-                        logger.warning(f"Could not backup local database: {e}")
-                self.download_database()
+            # Download if remote is newer or local doesn't exist
+            if not os.path.exists(self.db_path) or remote_timestamp > local_timestamp:
+                logger.info(f"Remote is newer ({remote_timestamp} > {local_timestamp}); downloading...")
+                # Backup existing local file (optional)
+                if os.path.exists(self.db_path):
+                    import shutil
+                    backup_path = os.path.join(self._backup_dir, f"{os.path.basename(self.db_path)}.{int(time.time())}.bak")
+                    try:
+                        shutil.copy2(self.db_path, backup_path)
+                        logger.info(f"Local database backed up to {backup_path}")
+                    except Exception as e:
+                        logger.warning(f"Could not backup local database: {e}")
+                self.download_database()
@@
-        else:
-            # No Dropbox; ensure local file exists
-            if not os.path.exists(self.db_path):
-                open(self.db_path, 'w').close()
+        else:
+            # No Dropbox; ensure local file exists (do not truncate existing file)
+            if not os.path.exists(self.db_path):
+                # Create file atomically
+                open(self.db_path, 'x').close()
@@
-        self.init_database()
-        self.setup_cleanup_scheduler()
+        self.init_database()
+        self.setup_cleanup_scheduler()
+        # Start periodic backup/upload thread
+        self._start_periodic_backup()
@@
     def _ensure_db_writable(self):
@@
-        if not os.path.exists(self.db_path):
-            try:
-                open(self.db_path, 'w').close()
-                os.chmod(self.db_path, 0o666)
-                logger.info(f"Created new database file: {self.db_path}")
-            except Exception as e:
-                logger.error(f"Could not create database file: {e}")
+        if not os.path.exists(self.db_path):
+            try:
+                # Create file safely without truncating existing ones
+                open(self.db_path, 'x').close()
+                os.chmod(self.db_path, 0o666)
+                logger.info(f"Created new database file: {self.db_path}")
+            except FileExistsError:
+                # Race: file was created between exists() and open('x')
+                logger.debug("Database file already created by another process")
+            except Exception as e:
+                logger.error(f"Could not create database file: {e}")
@@
     def _upload_to_dropbox(self):
@@
-        # Rate limit: at most once every 30 seconds, but skip if shutdown
-        now = time.time()
-        if now - self._last_upload < 30 and not self._shutdown:
-            logger.debug("Skipping upload – less than 30s since last upload.")
-            return
+        # Rate limit: at most once every 30 seconds, but skip if shutdown
+        now = time.time()
+        if now - self._last_upload < 30 and not self._shutdown:
+            logger.debug("Skipping upload – less than 30s since last upload.")
+            return
@@
             dropbox_path = f"/{os.path.basename(self.db_path)}"
-            with open(self.db_path, 'rb') as f:
-                self.dropbox_client.files_upload(
-                    f.read(),
-                    dropbox_path,
-                    mode=dropbox.files.WriteMode('overwrite')
-                )
+            # Upload via a temp file then move to avoid partial-overwrite issues
+            temp_remote = f"{dropbox_path}.uploading"
+            with open(self.db_path, 'rb') as f:
+                self.dropbox_client.files_upload(
+                    f.read(),
+                    temp_remote,
+                    mode=dropbox.files.WriteMode('overwrite')
+                )
+            try:
+                self.dropbox_client.files_move_v2(temp_remote, dropbox_path, autorename=False, allow_shared_folder=True)
+            except ApiError:
+                # If move fails because destination doesn't exist, fallback to copy or overwrite
+                try:
+                    self.dropbox_client.files_upload(open(self.db_path, 'rb').read(), dropbox_path, mode=dropbox.files.WriteMode('overwrite'))
+                except Exception as e:
+                    logger.error(f"Failed to finalize upload to Dropbox: {e}")
+                    raise
@@
             self._last_upload = now
-            logger.info(f"Successfully uploaded database to Dropbox: {dropbox_path}")
+            # Record local timestamp for remote file
+            try:
+                self._remote_timestamp = now
+                with open(self._local_timestamp_file, 'w') as f:
+                    f.write(str(now))
+            except Exception:
+                pass
+            logger.info(f"Successfully uploaded database to Dropbox: {dropbox_path}")
@@
     def download_database(self):
@@
-            # Download to local file
-            self.dropbox_client.files_download_to_file(self.db_path, dropbox_path)
+            # Download to a temp file and replace atomically
+            tmp_local = f"{self.db_path}.download_tmp"
+            self.dropbox_client.files_download_to_file(tmp_local, dropbox_path)
+            try:
+                os.replace(tmp_local, self.db_path)
+            except Exception:
+                # Fallback: copy
+                import shutil
+                shutil.copy2(tmp_local, self.db_path)
+                os.remove(tmp_local)
@@
-            # Ensure writable
-            try:
-                os.chmod(self.db_path, 0o666)
-                logger.info(f"Set permissions on {self.db_path} to 666 (read/write for all).")
-            except Exception as e:
-                logger.warning(f"Could not change permissions on {self.db_path}: {e}")
-            logger.info(f"Downloaded latest database from Dropbox: {dropbox_path}")
+            # Ensure writable
+            try:
+                os.chmod(self.db_path, 0o666)
+                logger.info(f"Set permissions on {self.db_path} to 666 (read/write for all).")
+            except Exception as e:
+                logger.warning(f"Could not change permissions on {self.db_path}: {e}")
+            logger.info(f"Downloaded latest database from Dropbox: {dropbox_path}")
@@
     def upload_database(self, force=False):
@@
         try:
             self._upload_to_dropbox()
         finally:
             if not force:
                 self._upload_lock.release()
+
+    def _start_periodic_backup(self):
+        """Start a background timer thread that saves backups locally and uploads periodically."""
+        def periodic():
+            while not self._shutdown:
+                try:
+                    # Create atomic local backup copy
+                    import shutil
+                    ts = int(time.time())
+                    backup_path = os.path.join(self._backup_dir, f"{os.path.basename(self.db_path)}.{ts}.bak")
+                    try:
+                        shutil.copy2(self.db_path, backup_path)
+                        logger.debug(f"Created local DB backup: {backup_path}")
+                    except Exception as e:
+                        logger.debug(f"Failed to create local backup: {e}")
+                    # Upload the backup file (non-blocking via thread lock)
+                    if self._upload_enabled:
+                        try:
+                            self.upload_database()
+                        except Exception as e:
+                            logger.debug(f"Periodic upload failed: {e}")
+                except Exception:
+                    logger.exception("Error in periodic backup loop")
+                time.sleep(self._backup_interval)
+
+        t = threading.Thread(target=periodic, daemon=True)
+        t.start()
