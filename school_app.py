import streamlit as st
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import pandas as pd
import hashlib
import random
import string
import datetime
import os
from io import BytesIO
from streamlit_calendar import calendar
from contextlib import contextmanager

# --- Constants & Config ---
DB_URL = st.secrets["database_url"]
# Admin & Registration Codes (Safely loaded from secrets)
# Fallback to defaults only if secrets are missing (useful for local dev without secrets.toml)
try:
    ADMIN_CODE = st.secrets["admin_code"]
except:
    ADMIN_CODE = "TEACHER2024" # Default fallback

try:
    SUPER_ADMIN_CODE = st.secrets["super_admin_code"]
except:
    SUPER_ADMIN_CODE = "MASTER2024" # Default fallback

# --- Translations ---
if 'lang' not in st.session_state:
    st.session_state['lang'] = 'English'

TRANSLATIONS = {
    'English': {
        'login': 'Login',
        'register': 'Register',
        'logout': 'Logout',
        'username': 'Username',
        'password': 'Password',
        'full_name': 'Full Name',
        'role': 'Role',
        'welcome': 'Welcome',
        'my_classes': 'My Classes',
        'my_inst': 'My Institution',
        'join_class': 'Join Class',
        'calendar': 'Calendar',
        'teacher_dash': 'Teacher Dashboard',
        'admin_dash': 'Admin Dashboard',
        'student_dash': 'Student Dashboard',
        'class_mgmt': 'Classes & Subjects',
        'assign_grading': 'Assignments & Grading',
        'global_gradebook': 'Global Gradebook',
        'user_mgmt': 'User Management',
        'inst_groups': 'Institutions & Groups',
        'biz_analytics': 'Business Analytics',
        'financial': 'Financial Tracking',
        'sys_status': 'System Status',
        'navigate': 'Navigate',
        'menu': 'Menu',
        'subject': 'Subject',
        'class': 'Class',
        'date': 'Date',
        'status': 'Status',
        'approve': 'Approve',
        'reject': 'Reject',
        'enrolled': 'Enrolled',
        'pending': 'Pending',
        'search': 'Search',
        'create': 'Create',
        'save': 'Save',
        'delete': 'Delete',
        'cancel': 'Cancel',
        'settings': 'Settings',
        'grading': 'Grading',
        'materials': 'Materials',
        'assignments': 'Assignments',
        'students': 'Students',
        'teachers': 'Teachers',
        'collaborators': 'Collaborators',
        'institution': 'Institution',
        'fee': 'Fee',
        'currency': 'Currency',
        'notes': 'Notes',
        'passing_grade': 'Passing Grade',
        'announcements': 'Announcements',
        'send': 'Send',
        'new_announcement': 'New Announcement',
        'author': 'Author',
        'content': 'Content',
        'title': 'Title',
        'no_announcements': 'No announcements yet.',
        'manage': 'Manage'
    },
    'Spanish': {
        'login': 'Iniciar Sesión',
        'register': 'Registrarse',
        'logout': 'Cerrar Sesión',
        'username': 'Usuario',
        'password': 'Contraseña',
        'full_name': 'Nombre Completo',
        'role': 'Rol',
        'welcome': 'Bienvenido/a',
        'my_classes': 'Mis Clases',
        'my_inst': 'Mi Institución',
        'join_class': 'Unirse a Clase',
        'calendar': 'Calendario',
        'teacher_dash': 'Panel de Profesor',
        'admin_dash': 'Panel de Admin',
        'student_dash': 'Panel de Estudiante',
        'class_mgmt': 'Clases y Materias',
        'assign_grading': 'Tareas y Calificaciones',
        'global_gradebook': 'Libreta de Calificaciones Global',
        'user_mgmt': 'Gestión de Usuarios',
        'inst_groups': 'Instituciones y Grupos',
        'biz_analytics': 'Análisis de Negocio',
        'financial': 'Seguimiento Financiero',
        'sys_status': 'Estado del Sistema',
        'navigate': 'Navegar',
        'menu': 'Menú',
        'subject': 'Materia',
        'class': 'Clase',
        'date': 'Fecha',
        'status': 'Estado',
        'approve': 'Aprobar',
        'reject': 'Rechazar',
        'enrolled': 'Inscritos',
        'pending': 'Pendiente',
        'search': 'Buscar',
        'create': 'Crear',
        'save': 'Guardar',
        'delete': 'Eliminar',
        'cancel': 'Cancelar',
        'settings': 'Configuración',
        'grading': 'Calificar',
        'materials': 'Materiales',
        'assignments': 'Tareas',
        'students': 'Estudiantes',
        'teachers': 'Profesores',
        'collaborators': 'Colaboradores',
        'institution': 'Institución',
        'fee': 'Tarifa',
        'currency': 'Moneda',
        'notes': 'Notas',
        'passing_grade': 'Nota de Aprobación',
        'announcements': 'Anuncios',
        'send': 'Enviar',
        'new_announcement': 'Nuevo Anuncio',
        'author': 'Autor',
        'content': 'Contenido',
        'title': 'Título',
        'no_announcements': 'No hay anuncios todavía.',
        'manage': 'Gestionar'
    }
}

def t(key):
    return TRANSLATIONS[st.session_state['lang']].get(key, key)

# --- Database Manager ---
class DatabaseManager:
    def __init__(self, db_url):
        self.db_url = db_url
        try:
            self.pool = pool.ThreadedConnectionPool(1, 20, self.db_url)
        except Exception as e:
            st.error(f"Failed to initialize connection pool: {e}")
            raise e
        self.init_db()
        self.check_and_migrate()

    @contextmanager
    def get_connection(self):
        conn = self.pool.getconn()
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            raise e
        else:
            conn.commit()
        finally:
            self.pool.putconn(conn)

    def init_db(self):
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    # Users
                    c.execute('''CREATE TABLE IF NOT EXISTS users (
                                id SERIAL PRIMARY KEY,
                                username TEXT UNIQUE NOT NULL,
                                password_hash TEXT NOT NULL,
                                full_name TEXT NOT NULL,
                                role TEXT NOT NULL,
                                is_active INTEGER DEFAULT 1,
                                group_name TEXT
                            )''')
                    
                    # Institutions
                    c.execute('''CREATE TABLE IF NOT EXISTS institutions (
                                id SERIAL PRIMARY KEY,
                                name TEXT UNIQUE NOT NULL
                            )''')

                    # Classes
                    c.execute('''CREATE TABLE IF NOT EXISTS classes (
                                id SERIAL PRIMARY KEY,
                                teacher_id INTEGER NOT NULL,
                                name TEXT NOT NULL,
                                access_code TEXT UNIQUE NOT NULL,
                                passing_grade REAL DEFAULT 6.0,
                                evaluation_system TEXT DEFAULT 'annual',
                                FOREIGN KEY (teacher_id) REFERENCES users(id)
                            )''')

                    # Subjects
                    c.execute('''CREATE TABLE IF NOT EXISTS subjects (
                                id SERIAL PRIMARY KEY,
                                teacher_id INTEGER NOT NULL,
                                name TEXT NOT NULL,
                                FOREIGN KEY (teacher_id) REFERENCES users(id)
                            )''')

                    # Class-Subject Link (course_subjects)
                    c.execute('''CREATE TABLE IF NOT EXISTS course_subjects (
                                id SERIAL PRIMARY KEY,
                                class_id INTEGER NOT NULL,
                                subject_id INTEGER NOT NULL,
                                teacher_id INTEGER NOT NULL,
                                weight_assignments INTEGER DEFAULT 40,
                                weight_exams INTEGER DEFAULT 60,
                                FOREIGN KEY (class_id) REFERENCES classes(id) ON DELETE CASCADE,
                                FOREIGN KEY (subject_id) REFERENCES subjects(id),
                                FOREIGN KEY (teacher_id) REFERENCES users(id)
                            )''')

                    # Enrollments
                    c.execute('''CREATE TABLE IF NOT EXISTS enrollments (
                                id SERIAL PRIMARY KEY,
                                student_id INTEGER NOT NULL,
                                course_id INTEGER NOT NULL,
                                last_read_announcement_id INTEGER DEFAULT 0,
                                FOREIGN KEY (student_id) REFERENCES users(id),
                                FOREIGN KEY (course_id) REFERENCES classes(id),
                                UNIQUE(student_id, course_id)
                            )''')

                    # Collaborators
                    c.execute('''CREATE TABLE IF NOT EXISTS class_collaborators (
                                id SERIAL PRIMARY KEY,
                                teacher_id INTEGER NOT NULL,
                                class_id INTEGER NOT NULL,
                                FOREIGN KEY (teacher_id) REFERENCES users(id),
                                FOREIGN KEY (class_id) REFERENCES classes(id),
                                UNIQUE(teacher_id, class_id)
                            )''')

                    # Join Requests
                    c.execute('''CREATE TABLE IF NOT EXISTS join_requests (
                                id SERIAL PRIMARY KEY,
                                user_id INTEGER NOT NULL,
                                class_id INTEGER NOT NULL,
                                role TEXT NOT NULL,
                                status TEXT DEFAULT 'pending',
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (user_id) REFERENCES users(id),
                                FOREIGN KEY (class_id) REFERENCES classes(id)
                            )''')

                    # Grading Categories
                    c.execute('''CREATE TABLE IF NOT EXISTS grading_categories (
                                id SERIAL PRIMARY KEY,
                                course_subject_id INTEGER NOT NULL,
                                name TEXT NOT NULL,
                                weight INTEGER NOT NULL,
                                FOREIGN KEY (course_subject_id) REFERENCES course_subjects(id) ON DELETE CASCADE
                            )''')

                    # Assignments
                    c.execute('''CREATE TABLE IF NOT EXISTS assignments (
                                id SERIAL PRIMARY KEY,
                                course_subject_id INTEGER NOT NULL,
                                title TEXT NOT NULL,
                                description TEXT,
                                deadline DATE NOT NULL,
                                type TEXT DEFAULT 'assignment',
                                category_id INTEGER,
                                period INTEGER DEFAULT 1,
                                submission_type TEXT DEFAULT 'digital',
                                FOREIGN KEY (course_subject_id) REFERENCES course_subjects(id) ON DELETE CASCADE
                            )''')

                    # Submissions
                    c.execute('''CREATE TABLE IF NOT EXISTS submissions (
                                id SERIAL PRIMARY KEY,
                                assignment_id INTEGER NOT NULL,
                                student_id INTEGER NOT NULL,
                                file_name TEXT,
                                file_data BYTEA,
                                submission_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                submission_link TEXT,
                                FOREIGN KEY (assignment_id) REFERENCES assignments(id),
                                FOREIGN KEY (student_id) REFERENCES users(id)
                            )''')

                    # Grades
                    c.execute('''CREATE TABLE IF NOT EXISTS grades (
                                id SERIAL PRIMARY KEY,
                                student_id INTEGER NOT NULL,
                                assignment_id INTEGER NOT NULL,
                                grade REAL,
                                feedback TEXT,
                                submission_id INTEGER,
                                FOREIGN KEY (student_id) REFERENCES users(id),
                                FOREIGN KEY (assignment_id) REFERENCES assignments(id),
                                UNIQUE(student_id, assignment_id)
                            )''')

                    # Announcements
                    c.execute('''CREATE TABLE IF NOT EXISTS announcements (
                                id SERIAL PRIMARY KEY,
                                class_id INTEGER NOT NULL,
                                teacher_id INTEGER NOT NULL,
                                title TEXT NOT NULL,
                                content TEXT NOT NULL,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (class_id) REFERENCES classes(id),
                                FOREIGN KEY (teacher_id) REFERENCES users(id)
                            )''')

                    # Materials
                    c.execute('''CREATE TABLE IF NOT EXISTS materials (
                                id SERIAL PRIMARY KEY,
                                course_subject_id INTEGER NOT NULL,
                                title TEXT NOT NULL,
                                description TEXT,
                                file_name TEXT,
                                file_data BYTEA,
                                link TEXT,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (course_subject_id) REFERENCES course_subjects(id) ON DELETE CASCADE
                            )''')

                    # Financials
                    c.execute('''CREATE TABLE IF NOT EXISTS group_tariffs (
                                group_name TEXT PRIMARY KEY,
                                monthly_fee REAL DEFAULT 0.0,
                                billing_currency TEXT DEFAULT 'USD',
                                notes TEXT
                            )''')

                conn.commit()
        except Exception as e:
            st.error(f"Error initializing the database: {e}")
            raise e

    def check_and_migrate(self):
        """Ensure all columns exist in the database with redundant checks."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as c:
                    # Robust column addition for course_subjects
                    c.execute("ALTER TABLE course_subjects ADD COLUMN IF NOT EXISTS weight_assignments INTEGER DEFAULT 40")
                    c.execute("ALTER TABLE course_subjects ADD COLUMN IF NOT EXISTS weight_exams INTEGER DEFAULT 60")
                    
                    # Robust column addition for users
                    c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1")
                    
                    # Robust column addition for classes
                    c.execute("ALTER TABLE classes ADD COLUMN IF NOT EXISTS passing_grade REAL DEFAULT 6.0")
                    c.execute("ALTER TABLE classes ADD COLUMN IF NOT EXISTS evaluation_system TEXT DEFAULT 'annual'")
                    
                    # Robust column addition for assignments
                    c.execute("ALTER TABLE assignments ADD COLUMN IF NOT EXISTS period INTEGER DEFAULT 1")
                    c.execute("ALTER TABLE assignments ADD COLUMN IF NOT EXISTS submission_type TEXT DEFAULT 'digital'")
                    
                    # Robust column addition for submissions
                    c.execute("ALTER TABLE submissions ADD COLUMN IF NOT EXISTS submission_link TEXT")
                    
                    conn.commit()
            return True, "Migration successful."
        except Exception as e:
            return False, f"Migration error: {str(e)}"

    @st.cache_data(ttl=60)
    def get_all_users(_self):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT id, username, full_name, role, is_active, group_name FROM users ORDER BY role, full_name")
                return c.fetchall()

    @st.cache_data(ttl=60)
    def get_user_counts_by_group(_self):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT group_name, role, COUNT(*) as count FROM users GROUP BY group_name, role")
                return c.fetchall()

    @st.cache_data(ttl=300)
    def get_institutions(_self):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM institutions")
                return c.fetchall()

    def register_user(self, username, password, full_name, role, group_name=None):
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        # Admins are active by default, others need approval
        is_active = 1 if role.lower() == 'admin' else 0
        try:
            with self.get_connection() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO users (username, password_hash, full_name, role, group_name, is_active) VALUES (%s, %s, %s, %s, %s, %s)",
                                 (username, password_hash, full_name, role, group_name, is_active))
                    conn.commit()
            self.get_all_users.clear()
            self.get_pending_users.clear()
            msg = "Registration successful! Please login." if is_active else "Registration successful! Awaiting admin approval."
            return True, msg
        except Exception as e:
            if "UNIQUE" in str(e) or "unique" in str(e):
                return False, "Username already taken."
            return False, f"Database Error: {str(e)}"

    def verify_user(self, username, password):
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM users WHERE username = %s AND password_hash = %s",
                                    (username, password_hash))
                user = c.fetchone()
        return user


    def update_user_status(self, user_id, is_active):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE users SET is_active = %s WHERE id = %s", (1 if is_active else 0, user_id))
                conn.commit()

    def update_user_password(self, user_id, new_password):
        password_hash = hashlib.sha256(new_password.encode()).hexdigest()
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE users SET password_hash = %s WHERE id = %s", (password_hash, user_id))
                conn.commit()

    def update_user_group(self, user_id, group_name):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE users SET group_name = %s WHERE id = %s", (group_name, user_id))
                conn.commit()
        self.get_all_users.clear()
        self.get_user_counts_by_group.clear()

    def bulk_status_update(self, role, is_active):
        """Kill switch or bulk activation"""
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE users SET is_active = %s WHERE role = %s", (1 if is_active else 0, role))
                conn.commit()

    def get_space_usage_by_group(self):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT u.group_name, SUM(LENGTH(s.file_data)) as size
                    FROM submissions s
                    JOIN users u ON s.student_id = u.id
                    WHERE s.file_data IS NOT NULL
                    GROUP BY u.group_name
                """)
                sub_usage = c.fetchall()
                
                c.execute("""
                    SELECT u.group_name, SUM(LENGTH(m.file_data)) as size
                    FROM materials m
                    JOIN course_subjects cs ON m.course_subject_id = cs.id
                    JOIN classes c ON cs.class_id = c.id
                    JOIN users u ON c.teacher_id = u.id
                    WHERE m.file_data IS NOT NULL
                    GROUP BY u.group_name
                """)
                mat_usage = c.fetchall()
                
                groups = {}
                for row in sub_usage:
                    g = row['group_name'] or "No Group"
                    groups[g] = groups.get(g, 0) + (row['size'] or 0)
                for row in mat_usage:
                    g = row['group_name'] or "No Group"
                    groups[g] = groups.get(g, 0) + (row['size'] or 0)
                    
                return groups

    def bulk_assign_group(self, user_ids, group_name):
        if not user_ids: return
        with self.get_connection() as conn:
            with conn.cursor() as c:
                placeholders = ",".join(["%s"] * len(user_ids))
                c.execute(f"UPDATE users SET group_name = %s WHERE id IN ({placeholders})", [group_name] + user_ids)
                conn.commit()
        self.get_all_users.clear()
        self.get_user_counts_by_group.clear()

    @st.cache_data(ttl=300)
    def get_group_tariffs(_self):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM group_tariffs")
                return c.fetchall()

    def update_group_tariff(self, name, fee, currency, notes):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO group_tariffs (group_name, monthly_fee, billing_currency, notes)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(group_name) DO UPDATE SET 
                        monthly_fee=excluded.monthly_fee, 
                        billing_currency=excluded.billing_currency,
                        notes=excluded.notes
                """, (name, fee, currency, notes))
                conn.commit()
        self.get_group_tariffs.clear()


    @st.cache_data(ttl=30)
    def get_pending_users(_self):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT id, username, full_name, role, group_name FROM users WHERE is_active = 0 ORDER BY id DESC")
                return c.fetchall()

    def approve_user(self, user_id, full_name, group_name):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE users SET full_name = %s, group_name = %s, is_active = 1 WHERE id = %s", 
                             (full_name, group_name, user_id))
                conn.commit()
        self.get_pending_users.clear()
        self.get_all_users.clear()
        self.get_user_counts_by_group.clear()

    def delete_user(self, user_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM users WHERE id = %s", (user_id,))
                conn.commit()
        self.get_all_users.clear()
        self.get_pending_users.clear()
        self.get_user_counts_by_group.clear()


    def add_institution(self, name):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                try:
                    c.execute("INSERT INTO institutions (name) VALUES (%s)", (name,))
                    conn.commit()
                    self.get_institutions.clear()
                    return True
                except:
                    return False

    def delete_institution(self, inst_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM institutions WHERE id = %s", (inst_id,))
                conn.commit()
        self.get_institutions.clear()

    # --- Joining & Collaboration Logic ---
    def enroll_student_by_id(self, student_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                try:
                    c.execute("INSERT INTO enrollments (student_id, course_id) VALUES (%s, %s)", (student_id, class_id))
                    conn.commit()
                    self.get_student_classes.clear()
                    return True
                except: return False

    def add_collaborator_by_id(self, teacher_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                try:
                    c.execute("INSERT INTO class_collaborators (teacher_id, class_id) VALUES (%s, %s)", (teacher_id, class_id))
                    conn.commit()
                    self.get_teacher_classes.clear()
                    self.get_class_collaborators.clear()
                    self.get_teacher_collaborations.clear()
                    return True
                except: return False

    def is_user_in_class(self, user_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                # Check owner
                c.execute("SELECT 1 FROM classes WHERE id=%s AND teacher_id=%s", (class_id, user_id))
                owner = c.fetchone()
                if owner: return True, "You are the owner of this class."
                
                # Check student
                c.execute("SELECT 1 FROM enrollments WHERE course_id=%s AND student_id=%s", (class_id, user_id))
                student = c.fetchone()
                if student: return True, "You are already enrolled as a student."
                
                # Check collaborator
                c.execute("SELECT 1 FROM class_collaborators WHERE class_id=%s AND teacher_id=%s", (class_id, user_id))
                collab = c.fetchone()
                if collab: return True, "You are already a collaborator in this class."
                
                return False, None

    def request_to_join(self, user_id, class_id, role):
        # 1. Check if already in class
        in_class, msg = self.is_user_in_class(user_id, class_id)
        if in_class:
            return False, msg

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                # 2. Check if pending request exists
                c.execute("SELECT 1 FROM join_requests WHERE user_id=%s AND class_id=%s AND status='pending'", (user_id, class_id))
                exists = c.fetchone()
                if exists:
                    return False, "You already have a pending request for this class."
                
                # 3. Create request
                c.execute("INSERT INTO join_requests (user_id, class_id, role) VALUES (%s, %s, %s)", (user_id, class_id, role))
                conn.commit()
                self.get_pending_requests_for_teacher.clear()
                self.get_all_pending_requests.clear()
                return True, "Request sent successfully!"

    @st.cache_data(ttl=30)
    def get_pending_requests_for_teacher(_self, teacher_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                # Requests for classes owned by this teacher
                c.execute("""
                    SELECT jr.*, u.full_name, u.username, c.name as class_name
                    FROM join_requests jr
                    JOIN users u ON jr.user_id = u.id
                    JOIN classes c ON jr.class_id = c.id
                    WHERE c.teacher_id = %s AND jr.status = 'pending'
                """, (teacher_id,))
                return c.fetchall()

    @st.cache_data(ttl=30)
    def get_all_pending_requests(_self):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT jr.*, u.full_name, u.username, c.name as class_name
                    FROM join_requests jr
                    JOIN users u ON jr.user_id = u.id
                    JOIN classes c ON jr.class_id = c.id
                    WHERE jr.status = 'pending'
                """)
                return c.fetchall()

    def handle_request(self, request_id, action):
        """action: 'approved' or 'rejected'"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM join_requests WHERE id = %s", (request_id,))
                req = c.fetchone()
                if not req: return False
                
                c.execute("UPDATE join_requests SET status = %s WHERE id = %s", (action, request_id))
                
                if action == 'approved':
                    if req['role'] == 'student':
                        c.execute("INSERT INTO enrollments (student_id, course_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (req['user_id'], req['class_id']))
                    else:
                        c.execute("INSERT INTO class_collaborators (teacher_id, class_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (req['user_id'], req['class_id']))
                conn.commit()
                self.get_pending_requests_for_teacher.clear()
                self.get_all_pending_requests.clear()
                self.get_student_classes.clear()
                self.get_teacher_classes.clear()
                self.get_class_collaborators.clear()
                self.get_teacher_collaborations.clear()
                return True

    @st.cache_data(ttl=60)
    def get_institution_stats(_self, inst_name):
        with _self.get_connection() as conn:
            with conn.cursor() as c: # Using regular cursor for counts
                c.execute("SELECT COUNT(*) FROM users WHERE group_name = %s AND role = 'teacher'", (inst_name,))
                teachers = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM users WHERE group_name = %s AND role = 'student'", (inst_name,))
                students = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM classes c JOIN users u ON c.teacher_id = u.id WHERE u.group_name = %s", (inst_name,))
                cls_count = c.fetchone()[0]
                return {"teachers": teachers, "students": students, "classes": cls_count}

    @st.cache_data(ttl=60)
    def get_institution_classes(_self, inst_name):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT c.*, u.full_name as owner_name 
                    FROM classes c
                    JOIN users u ON c.teacher_id = u.id
                    WHERE u.group_name = %s
                """, (inst_name,))
                return c.fetchall()

    @st.cache_data(ttl=60)
    def get_teacher_collaborations(_self, teacher_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT c.* FROM classes c
                    JOIN class_collaborators cc ON c.id = cc.class_id
                    WHERE cc.teacher_id = %s
                """, (teacher_id,))
                return c.fetchall()

    @st.cache_data(ttl=60)
    def get_class_collaborators(_self, class_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT u.id, u.full_name, u.username
                    FROM users u
                    JOIN class_collaborators cc ON u.id = cc.teacher_id
                    WHERE cc.class_id = %s
                """, (class_id,))
                return c.fetchall()

    def remove_collaborator_from_class(self, teacher_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM class_collaborators WHERE teacher_id = %s AND class_id = %s", (teacher_id, class_id))
                conn.commit()

    def enroll_teacher_by_code(self, teacher_id, code):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT id, teacher_id FROM classes WHERE access_code = %s", (code,))
                target = c.fetchone()
                if not target:
                    return False, "Invalid Code."
                
                if target['teacher_id'] == teacher_id:
                    return False, "You are the owner of this class."
                
                try:
                    c.execute("INSERT INTO class_collaborators (teacher_id, class_id) VALUES (%s, %s)", (teacher_id, target['id']))
                    conn.commit()
                    return True, "Joined successfully as collaborator!"
                except:
                    return False, "You are already a collaborator in this class."

    def create_class(self, teacher_id, name):
        access_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO classes (teacher_id, name, access_code) VALUES (%s, %s, %s)",
                             (teacher_id, name, access_code))
                conn.commit()
        # Clear cache for teacher's class list
        self.get_teacher_classes.clear()
        return access_code

    def create_subject(self, teacher_id, name):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO subjects (teacher_id, name) VALUES (%s, %s)", (teacher_id, name))
                conn.commit()
        # Clear cache for teacher's subject list
        self.get_teacher_subjects.clear()

    def link_subject_to_class(self, class_id, subject_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                try:
                    c.execute("INSERT INTO course_subjects (class_id, subject_id, teacher_id) SELECT %s, %s, teacher_id FROM classes WHERE id = %s", 
                                 (class_id, subject_id, class_id))
                    conn.commit()
                    self.get_class_subjects.clear()
                    return True
                except:
                    return False

    @st.cache_data(ttl=60)
    def get_teacher_classes(_self, teacher_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT * FROM classes WHERE teacher_id = %s
                    UNION
                    SELECT c.* FROM classes c
                    JOIN class_collaborators cc ON c.id = cc.class_id
                    WHERE cc.teacher_id = %s
                """, (teacher_id, teacher_id))
                return c.fetchall()

    @st.cache_data(ttl=60)
    def get_teacher_subjects(_self, teacher_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM subjects WHERE teacher_id = %s", (teacher_id,))
                return c.fetchall()
            
    @st.cache_data(ttl=60)
    def get_class_subjects(_self, class_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT cs.id as course_subject_id, s.name as subject_name, cs.weight_assignments, cs.weight_exams
                    FROM course_subjects cs
                    JOIN subjects s ON cs.subject_id = s.id
                    WHERE cs.class_id = %s
                """, (class_id,))
                return c.fetchall()

    def enroll_student(self, student_id, access_code):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT id, teacher_id FROM classes WHERE access_code = %s", (access_code,))
                course = c.fetchone()
                if not course:
                    return False, "Invalid Access Code (Class not found)"
                
                if course['teacher_id'] == student_id:
                    return False, "You are the owner of this class (Teachers cannot join as students)."

                try:
                    c.execute("INSERT INTO enrollments (student_id, course_id) VALUES (%s, %s)", 
                                 (student_id, course['id']))
                    conn.commit()
                    self.get_student_classes.clear()
                    return True, "Enrolled in Class successfully!"
                except:
                    return False, "You are already enrolled in this class."

    # --- Fetching & Helper Methods (Updated for V2) ---
    @st.cache_data(ttl=60)
    def get_student_classes(_self, student_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT c.* 
                    FROM classes c 
                    JOIN enrollments e ON c.id = e.course_id 
                    WHERE e.student_id = %s
                """, (student_id,))
                return c.fetchall()

    def create_assignment(self, course_subject_id, title, desc, deadline, atype, category_id=None, submission_type='digital', period=1):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO assignments (course_subject_id, title, description, deadline, type, category_id, submission_type, period) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (course_subject_id, title, desc, deadline, atype, category_id, submission_type, period))
                conn.commit()
        self.get_assignments_by_class.clear()
        self.get_course_subject_assignments.clear()

    def update_class_settings(self, class_id, passing_grade, system):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE classes SET passing_grade = %s, evaluation_system = %s WHERE id = %s", (passing_grade, system, class_id))
                conn.commit()
        self.get_teacher_classes.clear()

    def update_grading_group(self, group_id, name, weight):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE grading_categories SET name = %s, weight = %s WHERE id = %s", (name, weight, group_id))
                conn.commit()
        self.get_grading_categories.clear()

    def add_grading_category(self, course_subject_id, name, weight):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO grading_categories (course_subject_id, name, weight) VALUES (%s, %s, %s)",
                             (course_subject_id, name, weight))
                conn.commit()
        self.get_grading_categories.clear()

    @st.cache_data(ttl=60)
    def get_grading_categories(_self, course_subject_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM grading_categories WHERE course_subject_id = %s", (course_subject_id,))
                return c.fetchall()

    def delete_grading_category(self, category_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM grading_categories WHERE id = %s", (category_id,))
                c.execute("UPDATE assignments SET category_id = NULL WHERE category_id = %s", (category_id,))
                conn.commit()
        self.get_grading_categories.clear()

    def update_assignment_category(self, assignment_id, category_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE assignments SET category_id = %s WHERE id = %s", (category_id, assignment_id))
                conn.commit()

    def remove_student_from_class(self, student_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                # 1. Find all assignment IDs linked to this class
                c.execute("""
                    SELECT a.id FROM assignments a
                    JOIN course_subjects cs ON a.course_subject_id = cs.id
                    WHERE cs.class_id = %s
                """, (class_id,))
                assign_ids = [r[0] for r in c.fetchall()]
                
                if assign_ids:
                    # 2. Delete grades and submissions for these assignments for this student
                    placeholders = ",".join(["%s"] * len(assign_ids))
                    c.execute(f"DELETE FROM grades WHERE student_id = %s AND assignment_id IN ({placeholders})", 
                                 [student_id] + assign_ids)
                    c.execute(f"DELETE FROM submissions WHERE student_id = %s AND assignment_id IN ({placeholders})", 
                                 [student_id] + assign_ids)
                
                # 3. Finally, remove the enrollment
                c.execute("DELETE FROM enrollments WHERE student_id = %s AND course_id = %s", (student_id, class_id))
                conn.commit()

    def delete_assignment(self, assignment_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM assignments WHERE id = %s", (assignment_id,))
                c.execute("DELETE FROM submissions WHERE assignment_id = %s", (assignment_id,))
                c.execute("DELETE FROM grades WHERE assignment_id = %s", (assignment_id,))
                conn.commit()
        self.get_assignments_by_class.clear()
        self.get_course_subject_assignments.clear()

    def delete_class(self, class_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                # 1. Get all linked course_subjects to perform sub-cascades
                c.execute("SELECT id FROM course_subjects WHERE class_id = %s", (class_id,))
                cs_ids = [r[0] for r in c.fetchall()]
                
                for cs_id in cs_ids:
                    # Reuse the unlinking logic for each subject in the class
                    c.execute("DELETE FROM grades WHERE assignment_id IN (SELECT id FROM assignments WHERE course_subject_id = %s)", (cs_id,))
                    c.execute("DELETE FROM assignments WHERE course_subject_id = %s", (cs_id,))
                    c.execute("DELETE FROM materials WHERE course_subject_id = %s", (cs_id,))
                    c.execute("DELETE FROM grading_categories WHERE course_subject_id = %s", (cs_id,))
                    c.execute("DELETE FROM course_subjects WHERE id = %s", (cs_id,))
                
                c.execute("DELETE FROM enrollments WHERE course_id = %s", (class_id,))
                c.execute("DELETE FROM classes WHERE id = %s", (class_id,))
                conn.commit()

    def delete_subject(self, subject_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT id FROM course_subjects WHERE subject_id = %s", (subject_id,))
                cs_ids = [r[0] for r in c.fetchall()]
                
                for cs_id in cs_ids:
                    c.execute("DELETE FROM grades WHERE assignment_id IN (SELECT id FROM assignments WHERE course_subject_id = %s)", (cs_id,))
                    c.execute("DELETE FROM assignments WHERE course_subject_id = %s", (cs_id,))
                    c.execute("DELETE FROM materials WHERE course_subject_id = %s", (cs_id,))
                    c.execute("DELETE FROM grading_categories WHERE course_subject_id = %s", (cs_id,))
                    c.execute("DELETE FROM course_subjects WHERE id = %s", (cs_id,))
                
                c.execute("DELETE FROM subjects WHERE id = %s", (subject_id,))
                conn.commit()

    def unlink_subject_from_class(self, course_subject_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM grades WHERE assignment_id IN (SELECT id FROM assignments WHERE course_subject_id = %s)", (course_subject_id,))
                c.execute("DELETE FROM assignments WHERE course_subject_id = %s", (course_subject_id,))
                c.execute("DELETE FROM materials WHERE course_subject_id = %s", (course_subject_id,))
                c.execute("DELETE FROM grading_categories WHERE course_subject_id = %s", (course_subject_id,))
                c.execute("DELETE FROM course_subjects WHERE id = %s", (course_subject_id,))
                conn.commit()

    def submit_assignment(self, assignment_id, student_id, file_name, file_data, submission_link=None):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO submissions (assignment_id, student_id, file_name, file_data, submission_link) 
                    VALUES (%s, %s, %s, %s, %s)
                """, (assignment_id, student_id, file_name, file_data, submission_link))
                conn.commit()
        self.get_submissions_for_subject.clear()

    @st.cache_data(ttl=60)
    def get_course_subject_assignments(_self, course_subject_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM assignments WHERE course_subject_id = %s", (course_subject_id,))
                return c.fetchall()

    def update_subject_weights(self, course_subject_id, w_assign, w_exams):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE course_subjects SET weight_assignments = %s, weight_exams = %s WHERE id = %s",
                             (w_assign, w_exams, course_subject_id))
                conn.commit()
        self.get_class_subjects.clear()

    @st.cache_data(ttl=60)
    def get_submissions_for_subject(_self, course_subject_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT s.id as submission_id, s.file_name, s.submission_date, s.file_data, s.submission_link, s.student_id, s.assignment_id,
                           a.title as assignment_title, a.deadline, a.submission_type,
                           u.full_name as student_name,
                           g.grade, g.feedback
                    FROM submissions s
                    JOIN assignments a ON s.assignment_id = a.id
                    JOIN users u ON s.student_id = u.id
                    LEFT JOIN grades g ON s.id = g.submission_id 
                    WHERE a.course_subject_id = %s
                """, (course_subject_id,))
                return c.fetchall()

    def get_gradable_students(self, class_id, assignment_id):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT u.id as student_id, u.full_name,
                           g.grade, g.feedback, g.submission_id,
                           sub.file_name, sub.file_data, sub.submission_date, sub.submission_link
                    FROM users u
                    JOIN enrollments e ON u.id = e.student_id
                    LEFT JOIN grades g ON (u.id = g.student_id AND g.assignment_id = %s)
                    LEFT JOIN submissions sub ON (u.id = sub.student_id AND sub.assignment_id = %s)
                    WHERE e.course_id = %s
                """, (assignment_id, assignment_id, class_id))
                return c.fetchall()

    def update_assignment(self, assign_id, title, deadline, period, atype, submission_type):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    UPDATE assignments 
                    SET title = %s, deadline = %s, period = %s, type = %s, submission_type = %s
                    WHERE id = %s
                """, (title, deadline, period, atype, submission_type, assign_id))
                conn.commit()

    @st.cache_data(ttl=60)
    def get_assignments_by_class(_self, class_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT a.*, s.name as subject_name 
                    FROM assignments a
                    JOIN course_subjects cs ON a.course_subject_id = cs.id
                    JOIN subjects s ON cs.subject_id = s.id
                    WHERE cs.class_id = %s
                    ORDER BY a.deadline DESC
                """, (class_id,))
                return c.fetchall()

    def grade_assignment_direct(self, assignment_id, student_id, grade, feedback):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO grades (assignment_id, student_id, grade, feedback)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(assignment_id, student_id) 
                    DO UPDATE SET grade=excluded.grade, feedback=excluded.feedback
                """, (assignment_id, student_id, grade, feedback))
                conn.commit()
        self.get_grades_for_student_v2.clear()
        self.get_submissions_for_subject.clear()

    def add_material(self, cs_id, title, desc, file_name, file_data, link):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO materials (course_subject_id, title, description, file_name, file_data, link) VALUES (%s, %s, %s, %s, %s, %s)",
                             (cs_id, title, desc, file_name, file_data, link))
                conn.commit()
        self.get_materials.clear()

    @st.cache_data(ttl=60)
    def get_materials(_self, cs_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM materials WHERE course_subject_id = %s ORDER BY created_at DESC", (cs_id,))
                return c.fetchall()

    def delete_material(self, mid):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM materials WHERE id = %s", (mid,))
                conn.commit()
        self.get_materials.clear()
            
    @st.cache_data(ttl=60)
    def get_all_student_assignments_v2(_self, student_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT a.title, a.deadline, a.type, sub.name as subject_name, c.name as class_name
                    FROM assignments a
                    JOIN course_subjects cs ON a.course_subject_id = cs.id
                    JOIN subjects sub ON cs.subject_id = sub.id
                    JOIN classes c ON cs.class_id = c.id
                    JOIN enrollments e ON c.id = e.course_id
                    WHERE e.student_id = %s
                """, (student_id,))
                return c.fetchall()

    @st.cache_data(ttl=60)
    def get_all_teacher_assignments_v2(_self, teacher_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT a.title, a.deadline, a.type, sub.name as subject_name, c.name as class_name
                    FROM assignments a
                    JOIN course_subjects cs ON a.course_subject_id = cs.id
                    JOIN subjects sub ON cs.subject_id = sub.id
                    JOIN classes c ON cs.class_id = c.id
                    WHERE c.teacher_id = %s OR c.id IN (SELECT class_id FROM class_collaborators WHERE teacher_id = %s)
                """, (teacher_id, teacher_id))
                return c.fetchall()
    
    # ... (Keep existing simple helpers if generic, but remove old course usages)
            
    def grade_submission(self, submission_id, assignment_id, student_id, grade, feedback):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO grades (submission_id, assignment_id, student_id, grade, feedback)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(assignment_id, student_id) 
                    DO UPDATE SET grade=excluded.grade, feedback=excluded.feedback
                """, (submission_id, assignment_id, student_id, grade, feedback))
                conn.commit()
        self.get_grades_for_student_v2.clear()
        self.get_submissions_for_subject.clear()

    @st.cache_data(ttl=60)
    def get_grades_for_student_v2(_self, student_id, course_subject_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT g.grade, a.type, a.title, g.feedback, cat.name as category_name, cat.weight as category_weight
                    FROM grades g
                    JOIN assignments a ON g.assignment_id = a.id
                    LEFT JOIN grading_categories cat ON a.category_id = cat.id
                    WHERE g.student_id = %s AND a.course_subject_id = %s
                """, (student_id, course_subject_id))
                return c.fetchall()

    @st.cache_data(ttl=60)
    def get_class_gradebook(_self, class_id, course_subject_id):
        with _self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT u.id, u.full_name 
                    FROM users u
                    JOIN enrollments e ON u.id = e.student_id
                    WHERE e.course_id = %s
                """, (class_id,))
                students = c.fetchall()
                
                c.execute("SELECT id, title FROM assignments WHERE course_subject_id = %s", (course_subject_id,))
                assigns = c.fetchall()
                
                c.execute("""
                    SELECT student_id, assignment_id, grade
                    FROM grades
                    WHERE assignment_id IN (SELECT id FROM assignments WHERE course_subject_id = %s)
                """, (course_subject_id,))
                grades = c.fetchall()
                
                return students, assigns, grades

    def check_submission_status(self, assignment_id, student_id):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT * FROM submissions WHERE assignment_id = %s AND student_id = %s", 
                                    (assignment_id, student_id))
                return c.fetchone()
                
    def get_enrollments(self, class_id):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT u.id, u.username, u.full_name
                    FROM users u
                    JOIN enrollments e ON u.id = e.student_id
                    WHERE e.course_id = %s
                """, (class_id,))
                return c.fetchall()

    # --- Announcements Logic ---
    def create_announcement(self, class_id, teacher_id, title, content):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO announcements (class_id, teacher_id, title, content) VALUES (%s, %s, %s, %s)",
                             (class_id, teacher_id, title, content))
                conn.commit()

    def get_announcements_for_class(self, class_id):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT a.*, u.full_name as author_name 
                    FROM announcements a
                    JOIN users u ON a.teacher_id = u.id
                    WHERE a.class_id = %s
                    ORDER BY a.created_at DESC
                """, (class_id,))
                return c.fetchall()

    def get_unread_announcement_count(self, student_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT last_read_announcement_id FROM enrollments WHERE student_id = %s AND course_id = %s",
                                       (student_id, class_id))
                last_id_row = c.fetchone()
                if not last_id_row: return 0
                
                last_id = last_id_row['last_read_announcement_id']
                c.execute("SELECT COUNT(*) FROM announcements WHERE class_id = %s AND id > %s",
                                     (class_id, last_id))
                count = c.fetchone().values() # Get the only value
                return list(count)[0]
                 # Alternatively, use regular cursor for count

    def mark_announcements_as_read(self, student_id, class_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT MAX(id) FROM announcements WHERE class_id = %s", (class_id,))
                row = c.fetchone()
                max_id = row[0] if row else None
                if max_id:
                    c.execute("UPDATE enrollments SET last_read_announcement_id = %s WHERE student_id = %s AND course_id = %s",
                                 (max_id, student_id, class_id))
                    conn.commit()

    def update_announcement(self, ann_id, title, content):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE announcements SET title = %s, content = %s WHERE id = %s", (title, content, ann_id))
                conn.commit()

    def delete_announcement(self, ann_id):
        with self.get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM announcements WHERE id = %s", (ann_id,))
                conn.commit()

# --- Reused Helpers ---
def render_calendar(events):
    # Events format: [{'title': 'Math Exam', 'start': '2024-01-01', 'backgroundColor': '#FF0000'}]
    calendar_options = {
        "headerToolbar": {
            "left": "today prev,next",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek,timeGridDay,listMonth"
        },
        "initialView": "dayGridMonth",
    }
    calendar(events=events, options=calendar_options)

# --- Initialize ---
@st.cache_resource
def get_db():
    db_instance = DatabaseManager(DB_URL)
    return db_instance

db = get_db()
# Always run migration once per session outside the cache to be extra safe
if 'migration_done' not in st.session_state:
    db.check_and_migrate()
    st.session_state['migration_done'] = True


# --- Authentication Views ---
def login_page():
    st.title(f"Cloud Class ☁️ - {t('login')}")
    
    username = st.text_input(t('username'))
    password = st.text_input(t('password'), type="password")
    
    if st.button(t('login')):
        user = db.verify_user(username, password)
        if user:
            if user['is_active']:
                st.session_state['user'] = dict(user)
                st.rerun()
            else:
                st.warning("Your account is pending approval by an administrator. / Tu cuenta está pendiente de aprobación por un administrador.")
        else:
            st.error("Invalid credentials / Credenciales inválidas")

def register_page():
    st.title(t('register'))
    role = st.selectbox("I am a / Soy...", ["Student", "Teacher", "Admin"])
    
    # Institution Selection
    insts = db.get_institutions()
    inst_names = ["-"] + [i['name'] for i in insts]
    sel_inst = st.selectbox(t('my_inst'), inst_names)
    
    if role == "Teacher":
        code = st.text_input("Admin Code (Required for Teachers) / Código de Profesor")
        if code != ADMIN_CODE:
            st.warning("Incorrect Admin Code / Código Incorrecto")
            return
    elif role == "Admin":
        code = st.text_input("Super Admin Code (Required for Admins) / Código de Admin", type="password")
        if code != SUPER_ADMIN_CODE:
            st.warning("Access Denied / Acceso Denegado")
            return

    username = st.text_input(t('username'))
    full_name = st.text_input(t('full_name'))
    password = st.text_input(t('password'), type="password")
    
    if st.button(t('register')):
        g_name = sel_inst if sel_inst != "-" else None
        success, msg = db.register_user(username, password, full_name, role.lower(), g_name)
        if success:
            st.success(msg)
        else:
            st.error(msg)

def admin_dashboard(user):
    st.sidebar.title(f"🛡️ Admin: {user['full_name']}")
    
    adm_opts = {
        t('user_mgmt'): "User Management",
        t('inst_groups'): "Institutions & Groups",
        t('biz_analytics'): "Business Analytics",
        t('financial'): "Financial Tracking",
        t('sys_status'): "System Status"
    }
    choice_label = st.sidebar.radio(t('navigate'), list(adm_opts.keys()))
    choice = adm_opts[choice_label]
    
    if choice == "User Management":
        st.header(f"👥 {t('user_mgmt')}")
        
        adm_tab1, adm_tab2, adm_tab3 = st.tabs(["📋 User List", "➕ Create User", "🔔 Pending Approvals"])
        
        with adm_tab2:
            st.subheader("Create New Account")
            with st.form("admin_create_user"):
                c1, c2 = st.columns(2)
                new_u = c1.text_input("Username")
                new_f = c1.text_input("Full Name")
                new_p = c2.text_input("Password", type="password")
                new_r = c2.selectbox("Role", ["student", "teacher", "admin"])
                
                # Fetch institutions for dropdown
                insts = db.get_institutions()
                inst_names = ["-"] + [i['name'] for i in insts]
                new_g = st.selectbox("Assign to Institution", inst_names)
                
                if st.form_submit_button("✨ Create User"):
                    if new_u and new_p and new_f:
                        success, m = db.register_user(new_u, new_p, new_f, new_r)
                        if success:
                            if new_g != "-":
                                with db.get_connection() as conn:
                                    with conn.cursor(cursor_factory=RealDictCursor) as c:
                                        c.execute("SELECT id FROM users WHERE username=%s", (new_u,))
                                        u_info = c.fetchone()
                                        if u_info: db.update_user_group(u_info['id'], new_g)
                            st.success(f"User {new_u} created successfully!")
                        else:
                            st.error(m)
                    else:
                        st.warning("Please fill required fields.")

        with adm_tab1:
            users = db.get_all_users()
            st.write(f"**Total Users:** {len(users)}")
            
            # Filtering Layer
            filter_cols = st.columns(3)
            search = filter_cols[0].text_input("🔍 Search", placeholder="Name/Username...")
            role_f = filter_cols[1].selectbox("Filter Role", ["All", "Teacher", "Student", "Admin"])
            
            all_groups = sorted(list(set([u['group_name'] or "No Group" for u in users])))
            group_f = filter_cols[2].selectbox("Filter Group", ["All"] + all_groups)
            
            st.divider()
            
            # User List with filters
            for u in users:
                g_name = u['group_name'] or "No Group"
                if search.lower() not in u['full_name'].lower() and search.lower() not in u['username'].lower(): continue
                if role_f != "All" and u['role'].lower() != role_f.lower(): continue
                if group_f != "All" and g_name != group_f: continue
                    
                with st.container(border=True):
                    l_col1, l_col2, l_col3 = st.columns([3, 2, 2])
                    status_icon = "🟢" if u['is_active'] else "🔴"
                    l_col1.write(f"{status_icon} **{u['full_name']}** (@{u['username']})")
                    l_col1.caption(f"Role: {u['role'].title()} | Group: {g_name}")
                    
                    with l_col2:
                        if u['id'] != user['id']:
                            new_status = st.toggle("Active", value=bool(u['is_active']), key=f"tg_{u['id']}")
                            if new_status != bool(u['is_active']):
                                db.update_user_status(u['id'], new_status)
                                st.rerun()
                    with l_col3:
                        tools = st.columns(2)
                        with tools[0].popover("🔑"):
                            rp = st.text_input("New Password", type="password", key=f"pw_{u['id']}")
                            if st.button("Reset", key=f"btn_pw_{u['id']}"):
                                db.update_user_password(u['id'], rp)
                                st.success("OK")
                        with tools[1].popover("🏢"):
                            ng = st.text_input("Group", value=u['group_name'] or "", key=f"grp_{u['id']}")
                            if st.button("Save", key=f"btn_grp_{u['id']}"):
                                db.update_user_group(u['id'], ng)
                                st.rerun()

        with adm_tab3:
            st.subheader("🔔 Pending User Registrations")
            pending_users = db.get_pending_users()
            if not pending_users:
                st.success("No pending approvals!")
            else:
                for p in pending_users:
                    with st.container(border=True):
                        c1, c2 = st.columns([3, 2])
                        with c1:
                            st.write(f"**{p['full_name']}** (@{p['username']})")
                            st.caption(f"Role: {p['role'].title()} | Institution: {p['group_name'] or 'None'}")
                        with c2:
                            col_a, col_r = st.columns(2)
                            with col_a.popover("📝 Edit & Approve"):
                                with st.form(f"approve_u_{p['id']}"):
                                    app_f = st.text_input("Full Name", value=p['full_name'])
                                    insts = db.get_institutions()
                                    inst_names = ["-"] + [i['name'] for i in insts]
                                    curr_idx = inst_names.index(p['group_name']) if p['group_name'] in inst_names else 0
                                    app_g = st.selectbox("Institution", inst_names, index=curr_idx)
                                    if st.form_submit_button("✅ Approve"):
                                        db.approve_user(p['id'], app_f, app_g if app_g != "-" else None)
                                        st.success("User approved!")
                                        st.rerun()
                            if col_r.button("❌ Reject", key=f"rej_u_{p['id']}"):
                                db.delete_user(p['id'])
                                st.rerun()

    elif choice == "Institutions & Groups":
        st.header("🏢 Institutions Management")
        tab_list, tab_usage, tab_manage = st.tabs(["🏗️ Manage List", "📊 Space Usage", "🛠️ Bulk Assign"])
        
        with tab_list:
            st.subheader("Registered Institutions")
            with st.form("new_inst"):
                n_name = st.text_input("Institution Name", placeholder="e.g. Stanford University")
                if st.form_submit_button("➕ Register Institution"):
                    if n_name:
                        if db.add_institution(n_name): st.success("Registered!")
                        else: st.error("Already exists.")
                        st.rerun()

            st.write("---")
            insts = db.get_institutions()
            if not insts:
                st.info("No institutions registered.")
            else:
                for i in insts:
                    c1, c2 = st.columns([5, 1])
                    c1.write(f"🏢 **{i['name']}**")
                    with c2.popover("🗑️"):
                        st.warning(f"Delete '{i['name']}'? This doesn't delete users, only the name from this list.")
                        if st.button("Confirm Delete", key=f"del_i_{i['id']}"):
                            db.delete_institution(i['id'])
                            st.rerun()

        with tab_usage:
            usage_info = db.get_space_usage_by_group()
            if not usage_info: st.info("No storage data.")
            else:
                df_u = pd.DataFrame([{"Institution": k, "MB": round(v/(1024*1024), 2)} for k, v in usage_info.items()])
                st.bar_chart(df_u.set_index("Institution"))
                st.table(df_u)

        with tab_manage:
            st.subheader("Bulk Assign Groups")
            insts = db.get_institutions()
            if not insts:
                st.warning("Please register an institution first.")
            else:
                group_name = st.selectbox("Target Institution", [i['name'] for i in insts])
                users = db.get_all_users()
                u_opts = {f"{u['full_name']} (@{u['username']})": u['id'] for u in users}
                sel = st.multiselect("Select Users", list(u_opts.keys()))
                if st.button("Assign Selected to Group"):
                    if sel:
                        db.bulk_assign_group([u_opts[n] for n in sel], group_name)
                        st.success("Assigned!")
                        st.rerun()

    elif choice == "Business Analytics":
        st.header("📈 Business Intelligence")
        
        # Pending Approvals for Admin visibility
        st.subheader("🔔 Global Join Requests")
        reqs = db.get_all_pending_requests()
        if not reqs:
            st.info("No pending join requests.")
        else:
            for r in reqs:
                with st.container(border=True):
                    c1, c2, c3 = st.columns([3, 1, 1])
                    c1.write(f"**{r['full_name']}** (@{r['username']}) -> **{r['class_name']}** ({r['role']})")
                    if c2.button("Approve", key=f"adm_app_{r['id']}"):
                        db.handle_request(r['id'], 'approved')
                        st.rerun()
                    if c3.button("Reject", key=f"adm_rej_{r['id']}"):
                        db.handle_request(r['id'], 'rejected')
                        st.rerun()
        st.divider()

        stats = db.get_user_counts_by_group()
        if not stats:
            st.info("Not enough data for analytics.")
        else:
            df_st = pd.DataFrame([dict(r) for r in stats])
            
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Users per Institution")
                df_gp = df_st.groupby('group_name')['count'].sum().reset_index()
                st.bar_chart(df_gp.set_index('group_name'))
            
            with c2:
                st.subheader("Role Distribution")
                df_role = df_st.groupby('role')['count'].sum().reset_index()
                st.write(df_role)

            st.divider()
            st.subheader("Global Activity Metrics")
            m1, m2, m3 = st.columns(3)
            with db.get_connection() as conn:
                with conn.cursor() as c:
                    c.execute("SELECT (SELECT COUNT(*) FROM classes), (SELECT COUNT(*) FROM assignments), (SELECT COUNT(*) FROM submissions)")
                    row = c.fetchone()
                    m1.metric("Total Classes", row[0])
                    m2.metric("Total Assignments", row[1])
                    m3.metric("Total Submissions", row[2])

    elif choice == "Financial Tracking":
        st.header("💰 Financial & Tariff Management")
        st.caption("Internal data (Private to Admin)")
        
        all_users = db.get_all_users()
        groups = sorted(list(set([u['group_name'] for u in all_users if u['group_name']])))
        
        if not groups:
            st.warning("No groups defined yet. Assign users to institutions first.")
        else:
            col_list, col_edit = st.columns([1.5, 1])
            
            with col_edit:
                st.subheader("Edit Tariff")
                sel_g = st.selectbox("Select Institution", groups)
                # Fetch existing if any
                with db.get_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as c:
                        c.execute("SELECT * FROM group_tariffs WHERE group_name = %s", (sel_g,))
                        raw_t = c.fetchone()
                        
                with st.form("edit_tariff"):
                    fee = st.number_input("Monthly Fee", min_value=0.0, value=float(raw_t['monthly_fee']) if raw_t else 0.0)
                    curr = st.selectbox("Currency", ["USD", "ARS", "EUR", "MXN"], index=0)
                    notes = st.text_area("Notes", value=raw_t['notes'] if raw_t else "")
                    if st.form_submit_button("Save Billing Info"):
                        db.update_group_tariff(sel_g, fee, curr, notes)
                        st.success("Tariff updated.")
                        st.rerun()
            
            with col_list:
                st.subheader("Billing Overview")
                tariffs = db.get_group_tariffs()
                t_map = {t['group_name']: t for t in tariffs}
                
                rows = []
                for g in groups:
                    tariff_data = t_map.get(g)
                    count = sum(1 for v in all_users if v['group_name'] == g) # Changed u to v to avoid shadowing t() if I used it
                    rows.append({
                        "Institution": g,
                        "Users": count,
                        "Fee": f"{tariff_data['monthly_fee']} {tariff_data['billing_currency']}" if tariff_data else "Not Set",
                        "Notes": tariff_data['notes'] if tariff_data else ""
                    })
                st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    elif choice == "System Status":
        st.header("⚙️ System Status")
        st.info("Commercial Mode: Enabled")
        st.write("The database is currently running on Supabase (PostgreSQL).")
        st.success("Connection Status: ACTIVE (Pooled)")
        
        st.divider()
        st.subheader("Database Maintenance")
        if st.button("🔧 Force Database Schema Sync"):
            success, msg = db.check_and_migrate()
            if success: st.success(msg)
            else: st.error(msg)
            st.rerun()

def teacher_dashboard(user):
    st.sidebar.title(f"👨‍🏫 {t('teacher_dash')}: {user['full_name']}")
    
    teach_opts = {
        t('class_mgmt'): "Classes & Subjects",
        t('assign_grading'): "Assignments & Grading",
        t('global_gradebook'): "Global Gradebook",
        t('my_inst'): "My Institution",
        t('calendar'): "Calendar"
    }
    choice_label = st.sidebar.radio(t('navigate'), list(teach_opts.keys()))
    choice = teach_opts[choice_label]
    
    if choice == "My Institution":
        show_institution_view(user)
    elif choice == "Classes & Subjects":
        st.header(f"🏫 {t('class_mgmt')}")
        
        tab1, tab1_join, tab2, tab3 = st.tabs([f"🏗️ {t('class')}", f"🤝 {t('join_class')}", f"👤 {t('students')}", f"⚙️ {t('settings')}"])
        
        with tab1_join:
            st.subheader("Join an existing Class")
            st.caption("Enter a class code to join as a collaborator/assistant.")
            with st.form("join_teacher"):
                j_code = st.text_input("Class Code")
                if st.form_submit_button("Join Class"):
                    success, msg = db.enroll_teacher_by_code(user['id'], j_code)
                    if success: st.success(msg)
                    else: st.error(msg)
                    st.rerun()
        
        with tab1:
            st.subheader("1. Create a Class")
            with st.form("new_class"):
                 name = st.text_input("Class Name", placeholder="4th Grade A")
                 if st.form_submit_button("Create Class"):
                     db.create_class(user['id'], name)
                     st.success("Class created!")
                     st.rerun()
            
            st.divider()
            st.subheader("2. Manage Classes & Subjects")
            classes = db.get_teacher_classes(user['id'])
            all_teacher_subjects = db.get_teacher_subjects(user['id'])

            if not classes:
                st.info("No classes created yet.")
            
            for c in classes:
                with st.expander(f"📦 {c['name']} (Code: {c['access_code']})"):
                    # Row 1: Class Actions
                    c_cols = st.columns([5, 1])
                    c_cols[0].write(f"### Management: {c['name']}")
                    with c_cols[1].popover("🗑️ Class"):
                        st.warning("Delete this class and all links?")
                        if st.button("Confirm Delete Class", key=f"del_class_{c['id']}"):
                            db.delete_class(c['id'])
                            st.rerun()
                    
                    st.divider()
                    
                    # Row 2: Subjects inside this class
                    st.write("**Linked Subjects:**")
                    c_subjects = db.get_class_subjects(c['id'])
                    if not c_subjects:
                        st.caption("No subjects linked yet.")
                    else:
                        for sub in c_subjects:
                            s_cols = st.columns([4, 1])
                            s_cols[0].write(f"📚 {sub['subject_name']}")
                            with s_cols[1].popover("🗑️"):
                                st.error(f"Unlink '{sub['subject_name']}' from this class?")
                                st.caption("This deletes all assignments/grades for this subject in this class.")
                                if st.button("Confirm Unlink", key=f"unl_{sub['course_subject_id']}"):
                                    db.unlink_subject_from_class(sub['course_subject_id'])
                                    st.rerun()
                    
                    st.write("---")
                    
                    # Row 3: Add/Create Subjects
                    col_add, col_new = st.columns(2)
                    
                    with col_add:
                        st.write("🔗 **Link Existing Subjects**")
                        # Filter out already linked subjects
                        linked_ids = [s['subject_name'] for s in c_subjects] # Using name as key for simplicity in selectbox if IDs matched
                        available = {s['name']: s['id'] for s in all_teacher_subjects if s['name'] not in linked_ids}
                        
                        if not available:
                            st.caption("All your subjects are already linked.")
                        else:
                            with st.form(f"link_multi_{c['id']}"):
                                sel_multi = st.multiselect("Select Subjects", list(available.keys()))
                                if st.form_submit_button("Link Selected"):
                                    for sname in sel_multi:
                                        db.link_subject_to_class(c['id'], available[sname])
                                    st.success(f"Linked {len(sel_multi)} subjects!")
                                    st.rerun()

                    with col_new:
                        st.write("✨ **Create & Link New Subject**")
                        with st.form(f"quick_sub_{c['id']}"):
                            new_s_name = st.text_input("Subject Name")
                            if st.form_submit_button("Create & Link"):
                                if new_s_name:
                                    # Create globally for teacher
                                    db.create_subject(user['id'], new_s_name)
                                    # Get the most recently created subject for this teacher
                                    with db.get_connection() as conn:
                                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                            cur.execute("SELECT id FROM subjects WHERE teacher_id = %s AND name = %s ORDER BY id DESC", 
                                                         (user['id'], new_s_name))
                                            res = cur.fetchone()
                                    if res:
                                        db.link_subject_to_class(c['id'], res['id'])
                                        st.success(f"Created and linked {new_s_name}!")
                                        st.rerun()

        with tab2:
            st.subheader("Manage Students & Approvals")
            
            # Sub-tabs for clarity
            st_tab1, st_tab2, st_tab3 = st.tabs(["👥 Enrolled Students", "🔔 Pending Requests", "👨‍🏫 Collaborators"])
            
            with st_tab3:
                if classes:
                    c_opts_collab = {c['name']: dict(c) for c in classes}
                    sel_c_collab_name = st.selectbox("Select Class to see Teachers", list(c_opts_collab.keys()), key="sel_c_collab")
                    target_c = c_opts_collab[sel_c_collab_name]
                    target_class_id = target_c['id']
                    
                    # 1. Owner
                    with db.get_connection() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("SELECT full_name, username FROM users WHERE id = %s", (target_c['teacher_id'],))
                            owner = cur.fetchone()
                    st.write("### Owner / Principal")
                    st.write(f"⭐ **{owner['full_name']}** (@{owner['username']})")
                    
                    st.divider()
                    
                    # 2. Collaborators
                    st.write("### Collaborators & Helpers")
                    collabs = db.get_class_collaborators(target_class_id)
                    if not collabs:
                        st.info("No co-teachers or assistants in this class.")
                    else:
                        for cl in collabs:
                            cl_col1, cl_col2 = st.columns([4, 1])
                            cl_col1.write(f"🏃 **{cl['full_name']}** (@{cl['username']})")
                            # Only owner can remove collaborators
                            if user['id'] == target_c['teacher_id']:
                                with cl_col2.popover("🗑️"):
                                    st.write(f"Remove **{cl['full_name']}** as collaborator?")
                                    if st.button("Confirm", key=f"rem_cl_{cl['id']}_{target_class_id}"):
                                        db.remove_collaborator_from_class(cl['id'], target_class_id)
                                        st.rerun()

            with st_tab2:
                reqs = db.get_pending_requests_for_teacher(user['id'])
                if not reqs:
                    st.info("No pending requests for your classes.")
                else:
                    for r in reqs:
                        with st.container(border=True):
                            c1, c2, c3 = st.columns([3, 1, 1])
                            c1.write(f"**{r['full_name']}** (@{r['username']}) wants to join **{r['class_name']}** as {r['role']}")
                            if c2.button("Approve", key=f"app_{r['id']}"):
                                db.handle_request(r['id'], 'approved')
                                st.rerun()
                            if c3.button("Reject", key=f"rej_{r['id']}"):
                                db.handle_request(r['id'], 'rejected')
                                st.rerun()

            with st_tab1:
                if classes:
                    c_opts_manage = {c['name']: c['id'] for c in classes}
                    sel_c_manage = st.selectbox("Select Class to see Students", list(c_opts_manage.keys()), key="sel_c_manage")
                    target_class_id = c_opts_manage[sel_c_manage]
                    
                    # Fetch students for this class
                    with db.get_connection() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT u.id, u.full_name, u.username
                                FROM users u
                                JOIN enrollments e ON u.id = e.student_id
                                WHERE e.course_id = %s
                            """, (target_class_id,))
                            students = cur.fetchall()
                    
                    if not students:
                        st.info("No students enrolled in this class.")
                    else:
                        for s in students:
                            s_col1, s_col2 = st.columns([4, 1])
                            s_col1.write(f"👤 **{s['full_name']}** ({s['username']})")
                            with s_col2.popover("🗑️"):
                                st.write(f"Are you sure you want to remove **{s['full_name']}** from this class?")
                                if st.button("Confirm Removal", key=f"rem_std_{s['id']}_{target_class_id}"):
                                    db.remove_student_from_class(s['id'], target_class_id)
                                    st.success(f"Removed {s['full_name']}")
                                    st.rerun()

        with tab3:
            st.subheader("3. Class Settings")
            if classes:
                c_opts_set = {c['name']: dict(c) for c in classes}
                sel_c_set_name = st.selectbox("Select Class to Configure", list(c_opts_set.keys()))
                sel_c_obj = c_opts_set[sel_c_set_name]
                
                with st.form("class_settings"):
                    p_grade = st.number_input("Passing Grade", 0.0, 10.0, float(sel_c_obj.get('passing_grade', 6.0) or 6.0), step=0.5)
                    eval_sys = st.selectbox("Evaluation System", 
                                            ["annual", "semesters", "trimesters"], 
                                            index=["annual", "semesters", "trimesters"].index(sel_c_obj.get('evaluation_system', 'annual') or "annual"))
                    
                    if st.form_submit_button("Save Class Settings"):
                        db.update_class_settings(sel_c_obj['id'], p_grade, eval_sys)
                        st.success("Settings updated!")
                        st.rerun()
            else:
                st.info("Create a class first.")

    elif choice == "Assignments & Grading":
        classes = db.get_teacher_classes(user['id'])
        if not classes:
            st.warning("Create a class first.")
            return

        c_names = [c['name'] for c in classes]
        sel_c_name = st.selectbox("Select Class", c_names)
        sel_c = next(dict(c) for c in classes if c['name'] == sel_c_name)
        
        # Get subjects for this class
        class_subjects = db.get_class_subjects(sel_c['id'])
        if not class_subjects:
            st.warning("No subjects linked to this class yet.")
            return

        cs_map = {f"{row['subject_name']}": row for row in class_subjects}
        sel_cs_name = st.selectbox("Select Subject", list(cs_map.keys()))
        sel_cs = cs_map[sel_cs_name]
        
        st.info(f"{t('manage')}: **{sel_c_name} - {sel_cs_name}**")
        
        tab1, tab2, tab2_ann, tab3, tab4, tab5 = st.tabs([f"📝 {t('assignments')}", f"🎁 {t('materials')}", f"📢 {t('announcements')}", f"✅ {t('grading')}", f"📊 {t('global_gradebook')}", f"⚙️ {t('settings')}"])
        
        with tab1:
            st.subheader(f"{t('assignments')} Management")
            cats = db.get_grading_categories(sel_cs['course_subject_id'])
            
            if not cats:
                st.warning("⚠️ **Wait!** You need to create grading groups (e.g., 'Exams', 'HW') in the **'Groups Config'** tab first to organize your assignments.")
            else:
                with st.expander("➕ Create New Assignment"):
                    with st.form("new_assign_group"):
                        title = st.text_input("Title")
                        desc = st.text_area("Desc")
                        date = st.date_input("Deadline")
                        
                        c_opts = {c['name']: c['id'] for c in cats}
                        sel_cat_name = st.selectbox("Assign to Group", list(c_opts.keys()))
                        sel_cat_id = c_opts[sel_cat_name]
                        
                        # Period Selector
                        sys = sel_c.get('evaluation_system', 'annual')
                        if sys == 'semesters':
                            period = st.selectbox("Period", [1, 2], format_func=lambda x: f"Semester {x}")
                        elif sys == 'trimesters':
                            period = st.selectbox("Period", [1, 2, 3], format_func=lambda x: f"Quarter {x}")
                        else:
                            period = 1 # Annual
                        
                        atype = st.selectbox("Label/Type", ["assignment", "exam"])
                        stype = st.radio("Submission Type", ["digital", "physical"], horizontal=True, 
                                         help="Digital: Student must upload a file. Physical: Teacher grades directly (e.g. paper exam).")
                        
                        if st.form_submit_button("Create Assignment"):
                            db.create_assignment(sel_cs['course_subject_id'], title, desc, date, atype, sel_cat_id, stype, period)
                            st.success(f"Added to {sel_cat_name}!")
                            st.rerun()

            # Handle Orphaned Assignments (without category)
            all_assigns = db.get_course_subject_assignments(sel_cs['course_subject_id'])
            orphans = [a for a in all_assigns if a['category_id'] is None]
            if orphans:
                st.error("🚨 Found assignments without a group. Please move or delete them.")
                for a in orphans:
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                    c1.warning(f"Orphan: {a['title']}")
                    if cats:
                        with c3:
                            move_opts = {c['name']: c['id'] for c in cats}
                            sel_m = st.selectbox("Assign to", ["-"] + list(move_opts.keys()), key=f"orph_{a['id']}")
                            if sel_m != "-":
                                db.update_assignment_category(a['id'], move_opts[sel_m])
                                st.rerun()
                    if c4.button("🗑️", key=f"del_orph_{a['id']}"):
                        db.delete_assignment(a['id'])
                        st.rerun()

            st.write("---")
            # Display assignments grouped by category
            if not all_assigns:
                st.info("No assignments yet.")
            else:
                for cat in cats:
                    with st.expander(f"📁 Group: {cat['name']} ({cat['weight']}%)", expanded=True):
                        c_assigns = [a for a in all_assigns if a['category_id'] == cat['id']]
                        if not c_assigns:
                            st.caption("No assignments in this group.")
                        else:
                            for a in c_assigns:
                                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                                icon = "📝" if a['type'] == 'assignment' else "🎓"
                                mode = "📂" if a['submission_type'] == 'digital' else "📝 Physical"
                                col1.write(f"{icon} **{a['title']}** ({mode})")
                                col2.write(f"📅 {a['deadline']} (P{a['period']})")
                                
                                # Actions row
                                a_row = col3.columns(2)
                                with a_row[0].popover("✏️"):
                                    with st.form(f"edit_a_{a['id']}"):
                                        u_title = st.text_input("Title", value=a['title'])
                                        u_date = st.date_input("Deadline", value=pd.to_datetime(a['deadline']))
                                        sys_eval = sel_c.get('evaluation_system', 'annual')
                                        if sys_eval == 'semesters':
                                            u_period = st.selectbox("Period", [1, 2], index=max(0, min(1, a['period']-1)), format_func=lambda x: f"Semester {x}")
                                        elif sys_eval == 'trimesters':
                                            u_period = st.selectbox("Period", [1, 2, 3], index=max(0, min(2, a['period']-1)), format_func=lambda x: f"Quarter {x}")
                                        else:
                                            u_period = 1
                                        u_type = st.selectbox("Type", ["assignment", "exam"], index=0 if a['type'] == 'assignment' else 1)
                                        u_stype = st.radio("Submission", ["digital", "physical"], index=0 if a['submission_type'] == 'digital' else 1, horizontal=True)
                                        
                                        if st.form_submit_button("Save Changes"):
                                            db.update_assignment(a['id'], u_title, u_date, u_period, u_type, u_stype)
                                            st.rerun()

                                # Move option
                                other_cats = {c['name']: c['id'] for c in cats if c['id'] != cat['id']}
                                if other_cats:
                                    with a_row[1]:
                                        new_c = st.selectbox("Move", ["-"] + list(other_cats.keys()), key=f"move_{a['id']}", label_visibility="collapsed")
                                        if new_c != "-":
                                            db.update_assignment_category(a['id'], other_cats[new_c])
                                            st.rerun()
                                
                                with col4.popover("🗑️"):
                                    st.warning(f"Delete activity '{a['title']}'?")
                                    if st.button("Confirm Delete", key=f"del_a_{a['id']}"):
                                        db.delete_assignment(a['id'])
                                        st.rerun()

        with tab2:
            st.subheader(f"🎁 {t('materials')}")
            st.write("Upload PDFs, guides, or share links with your students.")
            
            with st.expander("➕ Add Material"):
                with st.form("new_material"):
                    m_title = st.text_input("Material Title")
                    m_desc = st.text_area("Short Description")
                    m_file = st.file_uploader("Upload File (Max 200MB default Streamlit)")
                    m_link = st.text_input("Web Link (optional)")
                    if st.form_submit_button("Add Material"):
                        f_name = m_file.name if m_file else None
                        f_data = m_file.read() if m_file else None
                        db.add_material(sel_cs['course_subject_id'], m_title, m_desc, f_name, f_data, m_link)
                        st.success("Material added!")
                        st.rerun()
            
            st.write("### Existing Materials")
            mats = db.get_materials(sel_cs['course_subject_id'])
            for m in mats:
                with st.container(border=True):
                    col1, col2 = st.columns([5, 1])
                    col1.markdown(f"**{m['title']}**")
                    if m['description']: col1.caption(m['description'])
                    
                    row2 = col1.columns(2)
                    if m['file_name']:
                        row2[0].download_button(f"📥 {m['file_name']}", m['file_data'], m['file_name'], key=f"dl_m_{m['id']}")
                    if m['link']:
                        row2[1].link_button("🔗 Link", m['link'])
                    
                    with col2.popover("🗑️"):
                        st.warning(f"Delete material '{m['title']}'?")
                        if st.button("Confirm Delete Material", key=f"del_m_{m['id']}"):
                            db.delete_material(m['id'])
                            st.rerun()

        with tab2_ann:
            st.subheader(f"📢 {t('announcements')}")
            with st.expander(f"✨ {t('new_announcement')}", expanded=False):
                with st.form("new_ann_form"):
                    ann_title = st.text_input(t('title'))
                    ann_content = st.text_area(t('content'))
                    if st.form_submit_button(t('send')):
                        if ann_title and ann_content:
                            db.create_announcement(sel_c['id'], user['id'], ann_title, ann_content)
                            st.success("Announcement posted!")
                            st.rerun()
            
            anns = db.get_announcements_for_class(sel_c['id'])
            if not anns:
                st.info(t('no_announcements'))
            else:
                for a in anns:
                    with st.container(border=True):
                        col_a1, col_a2 = st.columns([5, 1])
                        with col_a1:
                            st.markdown(f"### {a['title']}")
                            st.caption(f"📅 {a['created_at']} | ✍️ {a['author_name']}")
                            st.write(a['content'])
                        
                        # Authors or Class Owners can Edit/Delete
                        if a['teacher_id'] == user['id'] or sel_c['teacher_id'] == user['id']:
                            with col_a2:
                                # Edit using a popover
                                with st.popover("✏️"):
                                    with st.form(f"edit_ann_{a['id']}"):
                                        new_t = st.text_input(t('title'), value=a['title'])
                                        new_c = st.text_area(t('content'), value=a['content'])
                                        if st.form_submit_button(t('save')):
                                            db.update_announcement(a['id'], new_t, new_c)
                                            st.success("Updated!")
                                            st.rerun()
                                
                                # Delete with confirmation popover
                                with st.popover("🗑️"):
                                    st.warning(f"{t('delete')}?")
                                    if st.button(t('approve'), key=f"del_ann_{a['id']}"):
                                        db.delete_announcement(a['id'])
                                        st.success("Deleted!")
                                        st.rerun()

        with tab3:
            st.subheader("🎯 Grading Center")
            st.write("Correct pending tasks or edit previously assigned grades.")
            
            # Fetch ALL assignments for the CLASS to allow cross-subject filtering
            class_assigns = db.get_assignments_by_class(sel_c['id'])
            with db.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, full_name FROM users 
                        WHERE id IN (SELECT student_id FROM enrollments WHERE course_id = %s)
                    """, (sel_c['id'],))
                    students_in_class = cur.fetchall()
            
            if not class_assigns:
                st.info("No assignments found in any subject of this class.")
            else:
                # --- Filtering UI ---
                st.markdown("##### 🔍 Filters")
                f_col1, f_col2, f_col3 = st.columns(3)
                
                # Subject Filter
                sub_list = sorted(list(set([a['subject_name'] for a in class_assigns])))
                sel_f_sub = f_col1.selectbox("Filter by Subject", ["All"] + sub_list, index=0)
                
                # Student Filter
                std_names = {s['full_name']: s['id'] for s in students_in_class}
                sel_f_std = f_col2.selectbox("Filter by Student", ["All"] + list(std_names.keys()), index=0)
                
                # Activity Filter (dynamic based on subject)
                if sel_f_sub == "All":
                    filtered_a_for_list = class_assigns
                else:
                    filtered_a_for_list = [a for a in class_assigns if a['subject_name'] == sel_f_sub]
                
                act_list = sorted(list(set([a['title'] for a in filtered_a_for_list])))
                sel_f_act = f_col3.selectbox("Filter by Activity", ["All"] + act_list, index=0)
                
                # --- Logical Filtering ---
                pending_items = []
                graded_items = []
                
                for a in class_assigns:
                    # Apply Subject filter
                    if sel_f_sub != "All" and a['subject_name'] != sel_f_sub: continue
                    # Apply Activity filter
                    if sel_f_act != "All" and a['title'] != sel_f_act: continue
                    
                    gradables = db.get_gradable_students(sel_c['id'], a['id'])
                    for s in gradables:
                        # Apply Student filter
                        if sel_f_std != "All" and s['full_name'] != sel_f_std: continue
                        
                        show = False
                        if a['submission_type'] == 'physical': show = True
                        elif s['submission_date']: show = True
                        
                        if show:
                            item = {
                                "student_name": s['full_name'],
                                "student_id": s['student_id'],
                                "assign_title": a['title'],
                                "assign_id": a['id'],
                                "subject_name": a['subject_name'],
                                "type": a['type'],
                                "sub_type": a['submission_type'],
                                "grade": s['grade'],
                                "feedback": s['feedback'],
                                "file_name": s['file_name'],
                                "file_data": s['file_data'],
                                "date": s['submission_date'],
                                "submission_link": s['submission_link']
                            }
                            if s['grade'] is None:
                                pending_items.append(item)
                            else:
                                graded_items.append(item)

                # --- Display ---
                st.divider()
                st.markdown("#### ⚠️ Pending Correction")
                if not pending_items:
                    st.success("No pending items for the selected filters.")
                else:
                    for item in pending_items:
                        with st.expander(f"PENDING: {item['student_name']} - {item['assign_title']} ({item['subject_name']})"):
                            c1, c2 = st.columns([1, 1])
                            with c1:
                                st.write(f"**Subject:** {item['subject_name']}")
                                if item['sub_type'] == 'digital':
                                    st.write(f"**Submitted:** {item['date']}")
                                    if item['file_data']:
                                        st.download_button(f"📂 Download {item['file_name']}", item['file_data'], item['file_name'], key=f"p_dl_{item['student_id']}_{item['assign_id']}")
                                    if item['submission_link']:
                                        st.link_button("🔗 Open Submission Link", item['submission_link'])
                                else:
                                    st.info("Physical activity. No file involved.")
                            with c2:
                                with st.form(f"p_f_{item['student_id']}_{item['assign_id']}"):
                                    v = st.number_input("Grade", 0.0, 10.0, step=0.5)
                                    f = st.text_area("Feedback")
                                    if st.form_submit_button("Save Grade"):
                                        db.grade_assignment_direct(item['assign_id'], item['student_id'], v, f)
                                        st.rerun()

                st.divider()
                st.markdown("#### ✅ Corrected / History")
                if not graded_items:
                    st.caption("No graded items found for the selected filters.")
                else:
                    for item in graded_items:
                        with st.expander(f"GRADED [{item['grade']}]: {item['student_name']} - {item['assign_title']} ({item['subject_name']})"):
                            c1, c2 = st.columns([1, 1])
                            with c1:
                                st.write(f"**Subject:** {item['subject_name']}")
                                st.write(f"**Current Grade:** {item['grade']}")
                                if item['feedback']: st.write(f"**Feedback:** {item['feedback']}")
                                if item['sub_type'] == 'digital':
                                    if item['file_data']:
                                        st.download_button(f"📂 {item['file_name']}", item['file_data'], item['file_name'], key=f"g_dl_{item['student_id']}_{item['assign_id']}")
                                    if item['submission_link']:
                                        st.link_button("🔗 Submission Link", item['submission_link'])
                            with c2:
                                with st.form(f"g_f_{item['student_id']}_{item['assign_id']}"):
                                    v = st.number_input("Edit Grade", 0.0, 10.0, value=float(item['grade']), step=0.5)
                                    f = st.text_area("Edit Feedback", value=item['feedback'] or "")
                                    if st.form_submit_button("Update Revision"):
                                        db.grade_assignment_direct(item['assign_id'], item['student_id'], v, f)
                                        st.rerun()

        with tab4:
            st.subheader("Class Gradebook")
            cats = db.get_grading_categories(sel_cs['course_subject_id'])
            students, assignments, grades_list = db.get_class_gradebook(sel_c['id'], sel_cs['course_subject_id'])
            
            if not students:
                st.warning("No students enrolled.")
            elif not cats:
                st.info("Configure groups first.")
            else:
                grade_map = {(g['student_id'], g['assignment_id']): g['grade'] for g in grades_list}
                # Pre-fetch all grades per student for average calculation
                data = []
                for s in students:
                    row = {"Student": s['full_name']}
                    
                    # Get student raw grades to calc average
                    with db.get_connection() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT g.grade, a.category_id
                                FROM grades g 
                                JOIN assignments a ON g.assignment_id = a.id
                                WHERE g.student_id = %s AND a.course_subject_id = %s
                            """, (s['id'], sel_cs['course_subject_id']))
                            s_raw = cur.fetchall()
                    
                    st_final, cat_avgs = calculate_average_v3(s_raw, cats)
                    
                    # Show category averages in gradebook
                    for c_name, c_avg in cat_avgs.items():
                        row[f"Avg {c_name}"] = round(c_avg, 2) if c_avg is not None else "-"
                    
                    row["Final Progress"] = round(st_final, 2)
                    data.append(row)
                
                df_gb = pd.DataFrame(data)
                st.dataframe(df_gb, hide_index=True, use_container_width=True)

        with tab5:
            st.subheader("⚙️ Grading Groups")
            st.write("Edit names and weights. Total must be 100%.")
            
            with st.form("add_group"):
                st.caption("Add New Group")
                c1, c2 = st.columns([3, 1])
                new_cat = c1.text_input("Group Name")
                new_w = c2.number_input("Weight %", 0, 100, 25)
                if st.form_submit_button("➕ Create Group"):
                    db.add_grading_category(sel_cs['course_subject_id'], new_cat, new_w)
                    st.rerun()
            
            st.write("### Active Groups")
            cats = db.get_grading_categories(sel_cs['course_subject_id'])
            total_w = sum(c['weight'] for c in cats)
            
            if not cats:
                st.info("No groups created yet.")
            else:
                for c in cats:
                    with st.container(border=True):
                        col1, col2, col3 = st.columns([3, 1, 1.2])
                        with col1:
                            with st.form(f"edit_g_{c['id']}"):
                                u_name = st.text_input("Name", value=c['name'], label_visibility="collapsed")
                                u_weight = st.number_input("%", 0, 100, value=c['weight'], label_visibility="collapsed")
                                if st.form_submit_button("💾 Save"):
                                    db.update_grading_group(c['id'], u_name, u_weight)
                                    st.rerun()
                        
                        with col3:
                            with st.popover("🗑️"):
                                st.warning(f"Delete group '{c['name']}'?")
                                if st.button("Confirm Delete Group", key=f"del_cat_{c['id']}"):
                                    db.delete_grading_category(c['id'])
                                    st.rerun()
                
                if total_w != 100:
                    st.error(f"⚠️ Total weight is **{total_w}%**. Calculations will be inconsistent.")
                else:
                    st.success("✅ Weights sum to 100%.")

    elif choice == "Global Gradebook":
        st.header("📊 Global Class Gradebook")
        classes = db.get_teacher_classes(user['id'])
        if not classes:
            st.warning("Create a class first.")
            return

        c_names = {c['name']: dict(c) for c in classes}
        sel_c_name = st.selectbox("Select Class", list(c_names.keys()))
        sel_c = c_names[sel_c_name]
        passing = sel_c.get('passing_grade', 6.0) or 6.0
        sys = sel_c.get('evaluation_system', 'annual') or 'annual'
        
        st.info(f"System: **{sys.title()}** | Passing Grade: **{passing}**")

        # Get all subjs, students for this class
        subjects_in_class = db.get_class_subjects(sel_c['id'])
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT u.id, u.full_name FROM users u
                    JOIN enrollments e ON u.id = e.student_id
                    WHERE e.course_id = %s
                """, (sel_c['id'],))
                students_in_class = cur.fetchall()

        if not subjects_in_class or not students_in_class:
            if not subjects_in_class:
                st.warning("No subjects linked to this class yet. Go to 'Classes & Subjects' -> 'Link Subjects'.")
            if not students_in_class:
                st.warning("No students enrolled in this class yet.")
            return

        # Period Selection
        if sys == 'semesters':
            periods = {"Year Final": 0, "1st Semester": 1, "2nd Semester": 2}
        elif sys == 'trimesters':
            periods = {"Year Final": 0, "1st Quarter": 1, "2nd Quarter": 2, "3rd Quarter": 3}
        else:
            periods = {"Annual Progress": 1}
        
        sel_p_name = st.radio("View Period", list(periods.keys()), horizontal=True)
        sel_p_mode = periods[sel_p_name]

        # Build Matrix
        data = []
        for s in students_in_class:
            row = {"Student": s['full_name']}
            for sub in subjects_in_class:
                # Calculate avg for this student, this subject
                cats = db.get_grading_categories(sub['course_subject_id'])
                with db.get_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT g.grade, a.period, a.category_id
                            FROM grades g
                            JOIN assignments a ON g.assignment_id = a.id
                            WHERE g.student_id = %s AND a.course_subject_id = %s
                        """, (s['id'], sub['course_subject_id']))
                        raw_grades = cur.fetchall()

                if sel_p_mode == 0: # Year Final (Average of Period Averages)
                    p_avgs = []
                    num_p = 2 if sys == 'semesters' else 3
                    for p in range(1, num_p + 1):
                        p_grades = [g for g in raw_grades if g['period'] == p]
                        if p_grades:
                            p_f, _ = calculate_average_v3(p_grades, cats)
                            p_avgs.append(p_f)
                    
                    if p_avgs:
                        cell_val = round(sum(p_avgs) / len(p_avgs), 2)
                    else:
                        cell_val = 0.00
                else: # Specific Period or Annual
                    p_grades = [g for g in raw_grades if g['period'] == sel_p_mode]
                    p_avg_val, _ = calculate_average_v3(p_grades, cats)
                    cell_val = round(p_avg_val, 2)
                
                row[sub['subject_name']] = cell_val
            data.append(row)

        df_gb = pd.DataFrame(data)

        # Apply coloring
        def color_below_passing(val):
            try:
                # If it's a string (pre-formatted), convert back to float for comparison if needed
                f_val = float(val)
                if f_val < passing:
                    return 'color: #ff4b4b; font-weight: bold'
            except:
                pass
            return ''

        # Format numerical columns to 2 decimals
        cols_to_style = [c for c in df_gb.columns if c != "Student"]
        for col in cols_to_style:
            df_gb[col] = df_gb[col].map(lambda x: f"{x:.2f}")

        if cols_to_style:
            st.dataframe(df_gb.style.applymap(color_below_passing, subset=cols_to_style), 
                         hide_index=True, use_container_width=True)
        else:
            st.dataframe(df_gb, hide_index=True, use_container_width=True)

    elif choice == "Calendar":
        st.header("📅 Calendar")
        raw_assigns = db.get_all_teacher_assignments_v2(user['id'])
        events = []
        for a in raw_assigns:
            color = "#FF6C6C" if a['type'] == 'exam' else "#3788D8"
            events.append({
                "title": f"{a['title']} ({a['class_name']} - {a['subject_name']})",
                "start": str(a['deadline']),
                "backgroundColor": color
            })
        render_calendar(events)

def calculate_average_v3(grades, categories):
    """
    Calculates average based on custom category weights.
    Categories is a list of dicts: {'id': 1, 'name': 'TPs', 'weight': 50}
    """
    if not categories:
        # Fallback to simple average if no categories defined
        valid_grades = [g['grade'] for g in grades if g['grade'] is not None]
        return (sum(valid_grades)/len(valid_grades)) if valid_grades else 0.0, {}

    cat_averages = {}
    total_weight_used = 0
    calculated_score = 0
    
    for cat in categories:
        # Get grades for this category
        # Match by name or ID? Better by ID if available in grade row
        # Since 'grades' row comes from JOIN with assignments which has category_id
        c_grades = [g['grade'] for g in grades if g['category_id'] == cat['id'] and g['grade'] is not None]
        
        if c_grades:
            avg = sum(c_grades) / len(c_grades)
            cat_averages[cat['name']] = avg
            calculated_score += avg * cat['weight']
            total_weight_used += cat['weight']
        else:
            cat_averages[cat['name']] = None # No grades yet
            
    final_score = (calculated_score / total_weight_used) if total_weight_used > 0 else 0.0
    return final_score, cat_averages

def student_dashboard(user):
    st.sidebar.title(f"🎓 {t('student_dash')}: {user['full_name']}")
    
    stu_opts = {
        t('my_classes'): "My Classes",
        t('my_inst'): "My Institution",
        t('join_class'): "Join Class",
        t('calendar'): "Calendar"
    }
    choice_label = st.sidebar.radio(t('navigate'), list(stu_opts.keys()))
    choice = stu_opts[choice_label]
    
    if choice == "My Institution":
        show_institution_view(user)
    elif choice == "Join Class":
        st.header("Join a Class")
        with st.form("join_c"):
            code = st.text_input("Class Access Code")
            if st.form_submit_button("Join"):
                succ, msg = db.enroll_student(user['id'], code)
                if succ: st.success(msg)
                else: st.error(msg)

    elif choice == "My Classes":
        classes = db.get_student_classes(user['id'])
        if not classes:
            st.info("Not enrolled in any classes.")
            return
        
        c_names = [c['name'] for c in classes]
        sel_c_name = st.selectbox("Select Class", c_names)
        sel_c = next(dict(c) for c in classes if c['name'] == sel_c_name)
        
        subjects = db.get_class_subjects(sel_c['id'])
        if not subjects:
             st.info("No subjects in this class.")
             return

        # Show subjects as tabs? or list? Tabs is good.
        unread = db.get_unread_announcement_count(user['id'], sel_c['id'])
        ann_label = f"📢 {t('announcements')}"
        if unread > 0:
            ann_label += f" 🔴" # Notification dot

        s_tabs = st.tabs([ann_label, f"📊 {t('global_gradebook')}"] + [s['subject_name'] for s in subjects])
        
        with s_tabs[0]:
            st.subheader(f"📢 {t('announcements')}: {sel_c['name']}")
            # Mark as read
            db.mark_announcements_as_read(user['id'], sel_c['id'])
            
            anns = db.get_announcements_for_class(sel_c['id'])
            if not anns:
                st.info(t('no_announcements'))
            else:
                for a in anns:
                    with st.container(border=True):
                        st.markdown(f"### {a['title']}")
                        st.caption(f"📅 {a['created_at']} | ✍️ {a['author_name']}")
                        st.write(a['content'])

        with s_tabs[1]:
            st.subheader(f"📊 {t('global_gradebook')}: {sel_c['name']}")
            passing = sel_c.get('passing_grade', 6.0) or 6.0
            sys = sel_c.get('evaluation_system', 'annual') or 'annual'
            
            # Period Selection for Global Report
            if sys == 'semesters':
                periods = {"Year Final": 0, "1st Semester": 1, "2nd Semester": 2}
            elif sys == 'trimesters':
                periods = {"Year Final": 0, "1st Quarter": 1, "2nd Quarter": 2, "3rd Quarter": 3}
            else:
                periods = {"Annual Progress": 1}
            
            sel_p_name = st.radio("Report Period", list(periods.keys()), horizontal=True, key="std_global_p")
            sel_p_mode = periods[sel_p_name]

            # Build Summary Table
            summary_data = []
            for sub in subjects:
                # Calculate avg for this student, this subject
                cats = db.get_grading_categories(sub['course_subject_id'])
                with db.get_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT g.grade, a.period, a.category_id
                            FROM grades g
                            JOIN assignments a ON g.assignment_id = a.id
                            WHERE g.student_id = %s AND a.course_subject_id = %s
                        """, (user['id'], sub['course_subject_id']))
                        raw_grades = cur.fetchall()

                if sel_p_mode == 0: # Year Final
                    p_avgs = []
                    num_p = 2 if sys == 'semesters' else 3
                    for p in range(1, num_p + 1):
                        p_grades = [g for g in raw_grades if g['period'] == p]
                        if p_grades:
                            p_f, _ = calculate_average_v3(p_grades, cats)
                            p_avgs.append(p_f)
                    cell_val = round(sum(p_avgs) / len(p_avgs), 2) if p_avgs else 0.0
                else:
                    p_grades = [g for g in raw_grades if g['period'] == sel_p_mode]
                    p_avg_val, _ = calculate_average_v3(p_grades, cats)
                    cell_val = round(p_avg_val, 2)
                
                status = "✅ PASS" if cell_val >= passing else "❌ FAIL"
                summary_data.append({
                    "Subject": sub['subject_name'],
                    "Average": cell_val,
                    "Status": status
                })
            
            df_summary = pd.DataFrame(summary_data)
            
            def color_status(val):
                if val == "❌ FAIL": return 'color: #ff4b4b; font-weight: bold'
                if val == "✅ PASS": return 'color: #00c853; font-weight: bold'
                return ''

            # Format the average column to show 2 decimals
            df_summary['Average'] = df_summary['Average'].map(lambda x: f"{x:.2f}")

            st.dataframe(df_summary.style.applymap(color_status, subset=['Status']), 
                         hide_index=True, use_container_width=True)

        for i, sub in enumerate(subjects):
            with s_tabs[i+2]:
                st.subheader(f"{sub['subject_name']}")
                
                # Internal Tabs for Students: Activities vs Materials
                sub_tab1, sub_tab2, sub_tab3 = st.tabs(["📝 Activities", "📚 Materials", "📊 My Grades"])
                
                with sub_tab1:
                    assigns = db.get_course_subject_assignments(sub['course_subject_id'])
                    if not assigns:
                        st.info("No assignments.")
                    else:
                        # Separate Pending vs Done
                        pending = []
                        done = []
                        
                        for a in assigns:
                            raw_status = db.check_submission_status(a['id'], user['id'])
                            status = dict(raw_status) if raw_status else None
                            # For physical assignments, we check if there's a grade
                            if a['submission_type'] == 'physical':
                                with db.get_connection() as conn:
                                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                        cur.execute("SELECT grade FROM grades WHERE student_id = %s AND assignment_id = %s", (user['id'], a['id']))
                                        raw_g = cur.fetchone()
                                if raw_g: done.append((a, status, raw_g))
                                else: pending.append((a, status, None))
                            else:
                                if status: done.append((a, status, None))
                                else: pending.append((a, None, None))

                        done_tab, pending_tab = st.tabs([f"✅ Done ({len(done)})", f"⏳ Pending ({len(pending)})"])
                        
                        with pending_tab:
                            if not pending:
                                st.success("All caught up! No pending tasks.")
                            for a, status, _ in pending:
                                with st.expander(f"🔴 {a['title']} (Due: {a['deadline']})"):
                                    st.write(a['description'])
                                    if a['submission_type'] == 'physical':
                                        st.info("📝 **Physical Mode**: This activity is completed in class/on paper. No digital upload required.")
                                    else:
                                        st.write("Submit via file, link, or both:")
                                        up = st.file_uploader("Upload Work", key=f"u_{a['id']}")
                                        lnk = st.text_input("Submission Link (e.g. Google Drive, YouTube)", key=f"l_{a['id']}")
                                        if st.button("Submit My Work", key=f"b_{a['id']}"):
                                            if up or lnk:
                                                f_name = up.name if up else None
                                                f_data = up.read() if up else None
                                                db.submit_assignment(a['id'], user['id'], f_name, f_data, lnk)
                                                st.success("Uploaded successfully!")
                                                st.rerun()
                                            else:
                                                st.error("Please provide at least a file or a link.")

                        with done_tab:
                            if not done:
                                st.info("Nothing submitted yet.")
                            for a, status, grade_info in done:
                                with st.expander(f"🟢 {a['title']}"):
                                    st.write(a['description'])
                                    if a['submission_type'] == 'physical':
                                        st.success("📝 Graded by Teacher")
                                    else:
                                        st.success(f"✅ Submitted on: {status['submission_date']}")
                                        if status.get('submission_link'):
                                            st.info(f"Submitted link: {status['submission_link']}")
                                    
                                    # Show grade if exists
                                    with db.get_connection() as conn:
                                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                                            cur.execute("SELECT grade, feedback FROM grades WHERE student_id = %s AND assignment_id = %s", (user['id'], a['id']))
                                            raw_g = cur.fetchone()
                                    if raw_g:
                                        st.metric("Grade", f"{raw_g['grade']}/10")
                                        if raw_g['feedback']: st.caption(f"Feedback: {raw_g['feedback']}")
                                    else:
                                        st.info("Pending Correction (No grade yet)")

                with sub_tab2:
                    st.subheader("Subject Materials")
                    mats = db.get_materials(sub['course_subject_id'])
                    if not mats:
                        st.info("No materials shared yet.")
                    else:
                        for m in mats:
                            with st.container(border=True):
                                st.markdown(f"**{m['title']}**")
                                if m['description']: st.caption(m['description'])
                                r = st.columns(2)
                                if m['file_name']:
                                    r[0].download_button(f"📥 {m['file_name']}", m['file_data'], m['file_name'], key=f"std_m_{m['id']}")
                                if m['link']:
                                    r[1].link_button("🔗 Open Link", m['link'])

                with sub_tab3:
                    st.write("**My Progress & Grades**")
                    passing = sel_c.get('passing_grade', 6.0)
                    eval_sys = sel_c.get('evaluation_system', 'annual')
                    st.caption(f"Passing Grade: {passing} | System: {eval_sys.title()}")
                    
                    # Fetch Custom Categories (Groups)
                    sub_cats = db.get_grading_categories(sub['course_subject_id'])
                    with db.get_connection() as conn:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            cur.execute("""
                                SELECT g.grade, a.title, a.period, cat.id as category_id, cat.name as category_name, g.feedback
                                FROM grades g
                                JOIN assignments a ON g.assignment_id = a.id
                                LEFT JOIN grading_categories cat ON a.category_id = cat.id
                                WHERE g.student_id = %s AND a.course_subject_id = %s
                            """, (user['id'], sub['course_subject_id']))
                            sub_raw_grades = cur.fetchall()
                    
                    if sub_raw_grades:
                        # Breakdown by Period
                        if eval_sys == 'annual':
                            final, cat_avgs = calculate_average_v3(sub_raw_grades, sub_cats)
                            cols = st.columns(len(cat_avgs) + 1 if cat_avgs else 1)
                            for idx, (cname, cavg) in enumerate(cat_avgs.items()):
                                val = f"{cavg:.2f}" if cavg is not None else "-"
                                cols[idx].metric(cname, val)
                            cols[-1].metric("Annual Total", f"{final:.2f}")
                        else:
                            num_p = 2 if eval_sys == 'semesters' else 3
                            period_labels = ["1st Semester", "2nd Semester"] if eval_sys == 'semesters' else ["1st Quarter", "2nd Quarter", "3rd Quarter"]
                            
                            p_summaries = []
                            for p in range(1, num_p + 1):
                                p_data = [g for g in sub_raw_grades if g['period'] == p]
                                if p_data:
                                    p_final, _ = calculate_average_v3(p_data, sub_cats)
                                    p_summaries.append(p_final)
                                    st.metric(f"{period_labels[p-1]} Average", f"{p_final:.2f}")
                                else:
                                    st.caption(f"{period_labels[p-1]}: No grades yet.")
                            
                            if p_summaries:
                                year_final = sum(p_summaries) / len(p_summaries)
                                label = "PASSED" if year_final >= passing else "FAILED"
                                color = "normal" if year_final >= passing else "inverse"
                                st.metric("YEAR FINAL", f"{year_final:.2f}", delta=label, delta_color=color)
                        
                        st.subheader("Grades Detail")
                        df_sub = pd.DataFrame([dict(r) for r in sub_raw_grades])
                        st.dataframe(df_sub[['title', 'period', 'grade', 'feedback']], hide_index=True, use_container_width=True)
                    else:
                        st.info("No grades yet.")



    elif choice == "Calendar":
        st.header("📅 My Calendar")
        raw = db.get_all_student_assignments_v2(user['id'])
        events = []
        for a in raw:
            color = "#FF6C6C" if a['type'] == 'exam' else "#3788D8"
            events.append({
                "title": f"{a['title']} ({a['subject_name']})",
                "start": str(a['deadline']),
                "backgroundColor": color
            })
        render_calendar(events)

# --- Main App Logic ---
def main():
    # Language Switcher
    st.sidebar.title("🌐 Language / Idioma")
    lang = st.sidebar.selectbox("Select Language / Seleccione Idioma", ["English", "Spanish"], 
                                 index=0 if st.session_state['lang'] == 'English' else 1)
    if lang != st.session_state['lang']:
        st.session_state['lang'] = lang
        st.rerun()
    
    st.sidebar.divider()

    if 'user' not in st.session_state:
        menu_opts = [t('login'), t('register')]
        menu = st.sidebar.selectbox(t('menu'), menu_opts)
        if menu == t('login'):
            login_page()
        else:
            register_page()
    else:
        user = st.session_state['user']
        st.sidebar.title(f"{t('welcome')}, {user['full_name']}")
        if st.sidebar.button(t('logout')):
            del st.session_state['user']
            st.rerun()
            
        if user['role'] == 'teacher':
            teacher_dashboard(user)
        elif user['role'] == 'admin':
            admin_dashboard(user)
        else:
            student_dashboard(user)

def show_institution_view(user):
    st.header(f"🏢 {t('institution')}: {user['group_name'] or 'None'}")
    if not user['group_name']:
        st.info("You are not assigned to an institution yet.")
        return
        
    stats = db.get_institution_stats(user['group_name'])
    c1, c2, c3 = st.columns(3)
    c1.metric(t('teachers'), stats['teachers'])
    c2.metric(t('students'), stats['students'])
    c3.metric(f"Total {t('class')}es", stats['classes'])
    
    st.divider()
    st.subheader(f"Available {t('class')}es")
    classes = db.get_institution_classes(user['group_name'])
    if not classes:
        st.info("No classes found in your institution.")
    else:
        for c in classes:
            with st.container(border=True):
                col1, col2 = st.columns([4, 1])
                col1.write(f"📖 **{c['name']}**")
                col1.caption(f"Teacher: {c['owner_name']}")
                
                # Logic to show Request button or Joined status
                is_in, msg_in = db.is_user_in_class(user['id'], c['id'])
                
                # Check pending
                with db.get_connection() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("SELECT status FROM join_requests WHERE user_id=%s AND class_id=%s AND status='pending'", (user['id'], c['id']))
                        pending = cur.fetchone()
                
                if is_in:
                    col2.write(f"✅ **{t('enrolled')}**")
                    col2.caption(msg_in)
                elif pending:
                    col2.write(f"⏳ **{t('pending')}**")
                else:
                    if col2.button(t('join_class'), key=f"req_{c['id']}"):
                        success, resp = db.request_to_join(user['id'], c['id'], user['role'])
                        if success:
                            st.success(resp)
                            st.rerun()
                        else:
                            st.error(resp)

if __name__ == "__main__":
    main()
