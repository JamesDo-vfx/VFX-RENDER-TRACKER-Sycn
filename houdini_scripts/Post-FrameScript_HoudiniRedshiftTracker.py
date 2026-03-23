import requests
import hou
import time
import socket
import json
import os
import subprocess
import webbrowser

# --- CẤU HÌNH HỆ THỐNG (Được cập nhật bởi setup_config.bat) ---
# Không đọc trực tiếp từ config.json để file script có thể hoạt động độc lập khi copy sang máy khác.
WEBHOOK_URL        = "https://discord.com/api/webhooks/1466637236928909525/EE0ZjOqOx172NmLjJ94ryT-qvtwwGYxbQL62Yxr72D7URhtnPoEaGh8IQfwDMU2LSmoB"
FIREBASE_URL       = "https://vfx-machine-tracker-default-rtdb.asia-southeast1.firebasedatabase.app/renders.json"
GITHUB_WEBSITE_URL = "https://jamesdo-vfx.github.io/VFX-RENDER-TRACKER-Sycn/"
UPDATE_INTERVAL    = 30

def get_system_specs():
    """Lấy thông tin CPU, GPU và RAM sử dụng PowerShell (Thay thế wmic bị lỗi trên Win11)"""
    specs = {
        "cpu": "Unknown CPU",
        "gpu": "Unknown GPU",
        "ram": "Unknown RAM"
    }
    
    try:
        # 1. Lấy tên CPU qua PowerShell
        cpu_cmd = 'powershell -command "(Get-CimInstance Win32_Processor).Name"'
        cpu_info = subprocess.check_output(cpu_cmd, shell=True).decode().strip()
        if cpu_info: specs["cpu"] = cpu_info
            
        # 2. Lấy tên GPU qua PowerShell
        gpu_cmd = 'powershell -command "(Get-CimInstance Win32_VideoController).Name"'
        gpu_info = subprocess.check_output(gpu_cmd, shell=True).decode().strip()
        # Nếu có nhiều card, lấy card đầu tiên
        if gpu_info:
            specs["gpu"] = gpu_info.split('\r\n')[0].strip()
            
        # 3. Lấy dung lượng RAM qua PowerShell (Chuyển sang GB)
        ram_cmd = 'powershell -command "(Get-CimInstance Win32_PhysicalMemory | Measure-Object Capacity -Sum).Sum / 1GB"'
        ram_info = subprocess.check_output(ram_cmd, shell=True).decode().strip()
        if ram_info:
            # Xử lý trường hợp số thực dùng dấu phẩy tùy theo locale của Windows
            ram_val = ram_info.replace(',', '.')
            specs["ram"] = f"{round(float(ram_val))} GB"
            
    except Exception as e:
        print(f">>> [TRACKER] System Specs Warning: {e}")
        # Fallback cơ bản nếu PowerShell cũng lỗi
        try:
            import platform
            specs["cpu"] = platform.processor() or specs["cpu"]
        except: pass
        
    return specs

# (Already defined above)

def format_time(seconds):
    """Định dạng giây thành chuỗi thời gian dễ đọc"""
    if seconds < 0: return "0s"
    if seconds == 0: return "FINISHED"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"

def create_timeline_bar(current, start, end, size=15):
    """Tạo thanh tiến độ trực quan bằng emoji cho Discord"""
    total = max(end - start, 1)
    progress = max(0, min(1, (current - start) / float(total)))
    filled = int(round(size * progress))
    return f"{'🟩' * filled}{'⬛' * (size - filled)} **{int(progress * 100)}%**"

def main_sync():
    # 1. THU THẬP THÔNG TIN TỪ HOUDINI
    curr_frame = int(hou.frame())
    hip_name = hou.hipFile.basename()
    host_name = socket.gethostname()
    rop_node = hou.pwd()
    os_name = rop_node.name() # Tên node ROP ($OS)
    
    # Tự động nhận diện loại node để đổi tên hiển thị (v.d: File Cache)
    is_cache_job = False
    try:
        # Nếu là SOP node (trong Geo) hoặc chứa từ "cache" thì là cache job
        category = rop_node.type().category().name()
        type_name = rop_node.type().name().lower()
        if category == "Sop" or "cache" in type_name or "cache" in rop_node.path().lower():
            is_cache_job = True
    except:
        pass
    
    # Lấy Frame Range từ Global Playbar
    g_range = hou.playbar.frameRange()
    g_start = int(g_range[0])
    g_end = int(g_range[1])
    
    # Lấy Frame Range từ Node ROP hoặc SOP đang chạy
    try:
        if rop_node.parmTuple("f"):
            r_range = rop_node.evalParmTuple("f") 
            rop_start = int(r_range[0])
            rop_end = int(r_range[1])
        elif rop_node.parm("f1") and rop_node.parm("f2"):
            rop_start = int(rop_node.evalParm("f1"))
            rop_end = int(rop_node.evalParm("f2"))
        else:
            rop_start, rop_end = int(g_start), int(g_end)
    except:
        rop_start, rop_end = int(g_start), int(g_end)
    
    # Tạo key duy nhất cho bản ghi này trên Firebase (thay dấu '.' bằng '_')
    render_key = f"{host_name}_{os_name}".replace(".", "_")
    
    # Đường dẫn file output (đã expand biến số)
    output_path = "N/A"
    output_dir = "N/A"
    try:
        # 1. Danh sách các parameter chứa output path trên các loại node (ROP, LOPS, SOP Cache)
        # Sắp xếp theo thứ tự ưu tiên
        parm_candidates = [
            "vm_picture", "picture", "sopoutput", "file", 
            "productName", "RS_outputFileNamePrefix", 
            "RS_render_out_filename", "output_path", "output_file",
            "render_path", "export_path", "basefile", "outfile"
        ]
        
        raw_output = None
        for p_name in parm_candidates:
            parm = rop_node.parm(p_name)
            if parm:
                val = parm.eval()
                if val and val != "":
                    raw_output = val
                    break
        
        # Nếu chưa tìm thấy, quét thông minh qua các parms có tên liên quan đến output/path
        if not raw_output:
            for p in rop_node.parms():
                p_name_low = p.name().lower()
                if any(x in p_name_low for x in ["output", "path", "picture", "file"]):
                    if p.parmTemplate().type() == hou.parmTemplateType.String:
                        p_val = p.eval()
                        if p_val and ("/" in p_val or "\\" in p_val or "." in p_val):
                            raw_output = p_val
                            break

        # 2. Xử lý output_dir riêng nếu có parameter này (thường thấy trong các HDA tùy chỉnh)
        parm_dir = rop_node.parm("output_dir")
        if parm_dir:
            output_val = parm_dir.eval()
            if output_val:
                output_dir = hou.expandString(output_val).replace("\\", "/")
            
        if raw_output:
            output_path = hou.expandString(raw_output).replace("\\", "/")
            # Nếu path tương đối, thêm $HIP
            if not os.path.isabs(output_path.split(' ')[0]) and ":" not in output_path:
                output_path = (hou.expandString("$HIP/") + output_path).replace("//", "/")
            
            # Nếu chưa có output_dir từ tham số trực tiếp, lấy từ output_path
            if output_dir == "N/A":
                output_dir = os.path.dirname(output_path).replace("\\", "/")
        
        # Fallback cuối cùng nếu vẫn không thấy output_dir
        if output_dir == "N/A":
            output_dir = hou.expandString("$HIP/render").replace("\\", "/")
            
        # print(f">>> [TRACKER] Detected Output: {output_path}")
    except Exception as e:
        print(f">>> [TRACKER] Output Path Detection Error: {e}")

    # --- HỆ THỐNG LƯU TRỮ PHIÊN BẢN (PERSISTENCE) ---
    # Thay vì hou.session (gián đoạn khi tắt Houdini), ta dùng file log ở $HIP/render
    hip_dir = hou.expandString("$HIP")
    render_dir = os.path.join(hip_dir, "render")
    if not os.path.exists(render_dir):
        try:
            os.makedirs(render_dir)
        except OSError:
            pass
    log_file = os.path.join(render_dir, ".render_tracker_logs.json")
    all_jobs_data = {}
    
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                all_jobs_data = json.load(f)
        except: pass

    job_info = all_jobs_data.get(render_key, {})
    current_time = time.time()

    # 2. TÍNH TOÁN TIẾN ĐỘ & ETA
    # Tự động nhận diện: Nếu render đúng frame bắt đầu (rop_start) -> Xóa log cũ.
    # Nếu render từ giữa chừng (User skip frames) -> Giữ log cũ.
    is_fresh_start = (curr_frame == rop_start) or ('start_time' not in job_info)
    
    if is_fresh_start:
        # Chỉ xóa history khi thực sự là frame bắt đầu của dải frame
        if (curr_frame == rop_start) or ('frames_history' not in job_info):
            job_info['frames_history'] = []
            job_info['frames_done_count'] = 0
            job_info['total_render_time'] = 0
            job_info['ema_frame_time'] = 0
            job_info['start_time'] = current_time
            print(f">>> [TRACKER] New render detected at frame {curr_frame}. Cleaning history.")
            print(f">>> [TRACKER] Log File: {log_file}")
            if GITHUB_WEBSITE_URL:
                print(">>> [TRACKER] Dashboard Link (Click below):")
                print(GITHUB_WEBSITE_URL) # In dòng riêng để Console dễ nhận diện link
                # Tùy chọn: Tự động mở web khi render bắt đầu (Xóa dấu # ở dưới nêú muốn)
                # webbrowser.open(GITHUB_WEBSITE_URL)
            print(f">>> [TRACKER] File: {hip_name} | Node: {os_name} | Machine: {host_name}")
            if is_cache_job:
                print(">>> [TRACKER] Job Mode: CACHE")
            print(f">>> [TRACKER] Range: {rop_start} -> {rop_end} (Total: {rop_end - rop_start + 1} frames)")

        job_info['last_frame_time'] = current_time
        eta_str = "Calculating..."
        progress_val = 0
    else:
        # Tính toán thời gian thực tế cho frame vừa xong
        last_time = job_info.get('last_frame_time', job_info['start_time'])
        delta = current_time - last_time
        
        # Nếu khoảng cách giữa 2 lần cập nhật > 10 phút, coi như là vừa bấm Render lại (pause/resume)
        if delta < 600: 
            job_info['total_render_time'] = job_info.get('total_render_time', 0) + delta
            job_info['frames_done_count'] = job_info.get('frames_done_count', 0) + 1
            
            # --- THUẬT TOÁN EMA (STABLE EST) ---
            # Alpha = 0.2 giúp lấy trung bình trượt có trọng số, ổn định hơn simple average
            alpha = 0.2
            old_ema = job_info.get('ema_frame_time', 0)
            
            # Lọc nhiễu (Outlier filtering): 
            # Giới hạn frame time không quá 3x so với trung bình hiện tại để tránh spike đột ngột
            if old_ema > 0:
                effective_delta = min(delta, old_ema * 3.0)
            else:
                effective_delta = delta

            if old_ema == 0:
                new_ema = effective_delta
            else:
                new_ema = (alpha * effective_delta) + ((1 - alpha) * old_ema)
            
            job_info['ema_frame_time'] = new_ema

            # --- LƯU LỊCH SỬ CHI TIẾT TỪNG FRAME ---
            frame_record = {
                "frame": curr_frame,
                "render_time": format_time(delta),
                "render_seconds": round(delta, 2),
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "worker": host_name,
                "status": "Success"
            }
            if 'frames_history' not in job_info: job_info['frames_history'] = []
            job_info['frames_history'].append(frame_record)
            
            # Chỉ giữ lại 1000 frame gần nhất
            if len(job_info['frames_history']) > 1000:
                job_info['frames_history'].pop(0)

        else:
            # Nếu là resume, ta giữ nguyên EMA cũ hoặc cập nhật last_frame_time
            pass
            
        job_info['last_frame_time'] = current_time

        # Dự đoán thời gian còn lại (ETA) dựa trên EMA
        frames_done = job_info.get('frames_done_count', 0)
        total_rop_frames = max(1, rop_end - rop_start)
        remaining_frames = rop_end - curr_frame
        
        eta_str = "Calculating..."
        eft_str = "N/A"
        
        if remaining_frames <= 0:
            eta_str = "DONE"
            progress_val = 100
        elif frames_done > 0:
            # Sử dụng EMA frame time thay vì total_avg để ổn định EST
            avg_per_frame = job_info.get('ema_frame_time', delta)
            eta_val = avg_per_frame * remaining_frames
            eta_str = format_time(eta_val)
            
            # Tính thời điểm hoàn thành dự kiến
            finish_ts = current_time + eta_val
            eft_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(finish_ts))
            
            job_info['avg_frame_time'] = round(avg_per_frame, 2)
            job_info['estimated_finish_time'] = eft_str
            
            progress_val = int(max(0.0, min(100.0, ((curr_frame - rop_start + 1) / float(total_rop_frames + 1)) * 100.0)))
        else:
            progress_val = int(max(0.0, min(100.0, ((curr_frame - rop_start) / float(total_rop_frames)) * 100.0)))

    status_name = "Rendering"
    if curr_frame >= rop_end:
        status_name = "Finished"
        progress_val = 100
        eta_str = "DONE"

    # Cập nhật thông tin đọc được cho con người (Readable)
    # Gán lại các giá trị quan trọng để chúng xuất hiện ở đầu file JSON (Python 3.7+ preserves order)
    new_info = {
        "hip_file": hip_name,
        "rop_name": os_name,
        "total_render_time_readable": format_time(job_info.get('total_render_time', 0)),
        "start_time_readable": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_info.get('start_time', current_time))),
        "last_frame_time_readable": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job_info.get('last_frame_time', current_time)))
    }
    # Gộp các thông tin cũ vào sau các thông tin quan trọng
    new_info.update(job_info)
    job_info = new_info

    def save_logs():
        all_jobs_data[render_key] = job_info
        try:
            with open(log_file, 'w') as f:
                json.dump(all_jobs_data, f, indent=4)
        except Exception as e:
            print(f"Log Save Error: {e}")

    # 3. CẬP NHẬT FIREBASE (DASHBOARD)
    # Tự động bỏ qua lấy thông tin specs nếu đã có trong log (tiết kiệm tài nguyên)
    if 'cpu' not in job_info or job_info.get('cpu') == "Unknown CPU":
        print(">>> [TRACKER] Fetching system specs...")
        specs = get_system_specs()
        job_info['cpu'] = specs["cpu"]
        job_info['gpu'] = specs["gpu"]
        job_info['ram'] = specs["ram"]

    # Lưu log vào file ngay lập tức để bảo toàn dữ liệu (bao gồm cả specs)
    save_logs()

    db_payload = {
        render_key: {
            "shotName": os_name,
            "is_cache": is_cache_job,
            "hipFile": hip_name,
            "workerName": host_name,
            "machine_name": host_name, # Alias cho dashboard cũ
            "currentFrame": curr_frame,
            "current_frame": curr_frame, # Alias
            "startFrame": rop_start,
            "endFrame": rop_end,
            "globalStart": g_start,
            "globalEnd": g_end,
            "eta": eta_str,
            "progress": progress_val,
            "status": status_name,
            "node_path": rop_node.path(),
            "output_path": output_path,
            "output_dir": output_dir,
            "project_name": hip_name, # Alias
            "cpu": job_info.get("cpu", "N/A"),
            "gpu": job_info.get("gpu", "N/A"),
            "ram": job_info.get("ram", "N/A"),
            "timestamp": {".sv": "timestamp"} # Server-side timestamp
        }
    }
    
    try:
        # Sử dụng PATCH để chỉ cập nhật key của máy này mà không xóa máy khác
        requests.patch(FIREBASE_URL, json=db_payload, timeout=5)
    except Exception as e:
        print(f"Firebase Update Error: {e}")

    # 4. CẬP NHẬT DISCORD (THÔNG BÁO)
    # Chỉ gửi lên Discord nếu frame là bắt đầu, kết thúc, hoặc đã qua UPDATE_INTERVAL
    last_upd = job_info.get('last_update', 0)
    job_info['last_update_readable'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_upd))
    is_critical_frame = (curr_frame == rop_start) or (curr_frame >= rop_end)
    
    if not is_critical_frame and (current_time - last_upd < UPDATE_INTERVAL):
        return

    discord_status = "✅ Finished" if curr_frame >= rop_end else "🟠 Rendering..."
    embed_color = 1752220 if curr_frame >= rop_end else 5763719

    payload = {
        "embeds": [{
            "title": f"🎬 {os_name}",
            "description": f"**[{'CACHE MODE' if is_cache_job else 'RENDER MODE'}]**",
            "color": embed_color,
            "fields": [
                {"name": "Houdini Project", "value": f"`{hip_name}`", "inline": True},
                {"name": "Worker Node", "value": f"`{host_name}`", "inline": True},
                {"name": "Status", "value": f"**{discord_status}**", "inline": True},
                {"name": "Output Path", "value": f"```{output_path}```", "inline": False},
                {"name": "Timeline", "value": create_timeline_bar(curr_frame, rop_start, rop_end), "inline": False},
                {"name": "Progress", "value": f"`{curr_frame} / {rop_end}` ({progress_val}%)", "inline": True},
                {"name": "Estimated ETA", "value": f"`{eta_str}`", "inline": True}
            ],
            "footer": {"text": f"VFX Pipeline Tracker | Global: {g_start}-{g_end}"},
            "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
        }]
    }

    try:
        # Nếu đã có tin nhắn trước đó, cập nhật thay vì tạo mới (nếu muốn)
        # Ở đây JamesDoServer thường thích tạo message mới hoặc update tùy cấu hình.
        # Chúng ta sẽ thử update nếu có msg_id trong session
        msg_id = job_info.get('msg_id')
        if msg_id:
            res = requests.patch(f"{WEBHOOK_URL}/messages/{msg_id}", json=payload, timeout=5)
            if res.status_code != 200: # Nếu message bị xóa hoặc lỗi, gửi tin mới
                r = requests.post(f"{WEBHOOK_URL}?wait=true", json=payload, timeout=5)
                if r.status_code == 200: job_info['msg_id'] = r.json().get('id')
        else:
            r = requests.post(f"{WEBHOOK_URL}?wait=true", json=payload, timeout=5)
            if r.status_code == 200: job_info['msg_id'] = r.json().get('id')
        
        job_info['last_update'] = current_time
        save_logs() # Lưu lại msg_id và last_update
    except Exception as e:
        print(f"Discord Webhook Error: {e}")

# Chạy script
main_sync()
