import sys
import io

# Đảm bảo in được emoji trên Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import requests
import time
import math
import json
import datetime

FIREBASE_URL = "https://vfx-machine-tracker-default-rtdb.asia-southeast1.firebasedatabase.app/"

def format_time(seconds):
    """Định dạng giây thành chuỗi thời gian dễ đọc"""
    if seconds < 0: return "0s"
    if seconds == 0: return "FINISHED"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"

def calculate_eta(task_dict, current_frame, start_frame, end_frame, default_frame_time=60.0):
    """Tính toán ETA ổn định bằng EMA giống như script Houdini thật"""
    current_time = time.time()
    last_time = task_dict.get('lastTime', task_dict['startTime'])
    delta = current_time - last_time
    
    # Ở bước đầu tiên, delta sẽ là 0 hoặc rất nhỏ, ta gán mặc định nếu cần
    if delta < 0.1: delta = default_frame_time # Sử dụng giá trị người dùng nhập

    # EMA Algorithm
    alpha = 0.2
    old_ema = task_dict.get('emaFrameTime', 0)
    
    if old_ema == 0:
        new_ema = delta
    else:
        # Lọc nhiễu
        effective_delta = min(delta, old_ema * 3.0)
        new_ema = (alpha * effective_delta) + ((1 - alpha) * old_ema)
    
    task_dict['emaFrameTime'] = new_ema
    task_dict['lastTime'] = current_time
    
    remaining_frames = end_frame - current_frame
    if remaining_frames < 0: return "FINISHED"
    
    eta_seconds = remaining_frames * new_ema
    return format_time(eta_seconds)

def start_simulation():
    print("--- 🛠️ HOUDINI RENDER SIMULATOR (V2) ---")
    
    # 1. Nhập thông tin cơ bản
    hip_file = input("Nhập tên file Project (VD: HoudiniTestCode.hip): ") or "HoudiniTestCode.hip"
    shot_name = input("Nhập tên Shot (VD: Redshift_ROP1): ") or "Redshift_ROP1"
    frame_in = int(input("Global Start Frame: ") or 1001)
    frame_out = int(input("Global End Frame: ") or 1080)
    total_frames = frame_out - frame_in + 1
    render_time = float(input("Thời gian render mỗi frame (giây): ") or 60)

    # 2. Nhập số lượng và tên máy
    num_machines = int(input("Số lượng máy muốn giả lập: ") or 1)
    machines = []
    for i in range(num_machines):
        m_name = input(f" Nhập tên máy {i+1} (Mặc định: Machine {i+1}): ") or f"Machine {i+1}"
        machines.append(m_name)

    # 3. Logic chia Timeline
    frames_per_machine = math.ceil(total_frames / num_machines)
    tasks = []
    for i, m in enumerate(machines):
        start = frame_in + (i * frames_per_machine)
        end = min(start + frames_per_machine - 1, frame_out)
        if start <= frame_out:
            tasks.append({
                "workerName": m, 
                "startFrame": start, 
                "endFrame": end, 
                "currentFrame": start,
                "startTime": time.time()
            })

    print(f"\n📊 Đã chia Timeline cho {len(tasks)} máy:")
    for t in tasks:
        print(f"  - {t['workerName']}: Frame {t['startFrame']} -> {t['endFrame']}")

    confirm = input("\nBắt đầu giả lập? (y/n): ")
    if confirm.lower() != 'y': return

    print("\n🚀 Đang gửi dữ liệu lên Firebase...")

    # 4. Vòng lặp Render
    max_steps = frames_per_machine
    for step in range(max_steps + 1):
        for t in tasks:
            if t["currentFrame"] <= t["endFrame"]:
                
                # Progress calculation
                total_local = t["endFrame"] - t["startFrame"] + 1
                done_local = t["currentFrame"] - t["startFrame"]
                progress = int((done_local / total_local) * 100)
                
                # ETA calculation
                eta = calculate_eta(t, t["currentFrame"], t["startFrame"], t["endFrame"], render_time)
                
                status = "Rendering"
                if t["currentFrame"] == t["endFrame"]:
                    status = "Finished"
                    progress = 100
                    eta = "DONE"

                payload = {
                    "shotName": shot_name,
                    "workerName": t["workerName"],
                    "machine_name": t["workerName"],
                    "hipFile": hip_file,
                    "project_name": hip_file,
                    "startFrame": t["startFrame"],
                    "endFrame": t["endFrame"],
                    "currentFrame": t["currentFrame"],
                    "current_frame": t["currentFrame"],
                    "globalStart": frame_in,
                    "globalEnd": frame_out,
                    "progress": progress,
                    "eta": eta,
                    "status": status,
                    "node_path": f"/out/{shot_name}",
                    "output_path": f"C:/Render/{shot_name}/{shot_name}.$F4.exr",
                    "output_dir": f"C:/Render/{shot_name}",
                    "cpu": "Intel Core i9-14900K",
                    "gpu": "NVIDIA GeForce RTX 4090",
                    "ram": "64 GB",
                    "timestamp": {".sv": "timestamp"}
                }
                
                # Gửi dữ liệu lên Firebase
                # Nếu là frame đầu tiên, có thể xóa trắng data cũ của máy này
                if t["currentFrame"] == t["startFrame"] and step == 0:
                    # Dùng PUT để ghi đè toàn bộ (xóa key cũ)
                    requests.put(f"{FIREBASE_URL}/renders/{t['workerName']}.json", json=payload)
                else:
                    # Dùng PATCH để cập nhật
                    requests.patch(f"{FIREBASE_URL}/renders/{t['workerName']}.json", json=payload)
                
                if t["currentFrame"] < t["endFrame"]:
                    t["currentFrame"] += 1
                else:
                    t["currentFrame"] += 999999 # Stop this worker

        print(f"  [>] Bước {step}/{max_steps}... Đang cập nhật Dashboard (Chờ {render_time}s/frame)")
        time.sleep(render_time) 

    print("\n✅ Hoàn thành giả lập render!")

if __name__ == "__main__":
    try:
        start_simulation()
    except KeyboardInterrupt:
        print("\nĐã dừng giả lập.")
