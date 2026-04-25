import os, sys, time, threading

import dearpygui.dearpygui as dpg

import libiso


## -- General helpers

def is_admin():

    try:
        if os.name == 'nt':
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin() == 1
        else:
            return os.getuid() == 0
    except Exception:
        return False


def get_dpi_scale():

    if os.name == 'nt':
        try:
            import ctypes
            # Tell Windows we want to handle our own DPI scaling
            ctypes.windll.shcore.SetProcessDpiAwareness(1)
            dpi = ctypes.windll.user32.GetDpiForSystem()
            return dpi / 96.0
        except Exception:
            return 1.0
    return 2.0 # Linux/macOS usually handle native scaling well, or default to 1.0


#  Global UI State
state = {
    'iso_path': '',
    'target_device': '',
    'is_burning': False,
    'abort_token': None,
    'drives': [],
    'admin': is_admin()
}


#  Background Drive Polling 
def drive_poller():
    ''' mimics the Windows WM_DEVICECHANGE hook by polling in the background '''

    def drive_display_name(d):

        size_gb = int(d.total_space_bytes / (1024 * 1024 * 1024))

        if d.label:
            drive_display_name = f'{d.hardware_model} - {d.label} - {size_gb} GB ({d.device_path})'
        else:
            drive_display_name = f'{d.hardware_model} - {size_gb} GB ({d.device_path})'

        return drive_display_name


    while True:
        if state['is_burning']:
            continue

        try:
            current_drives = libiso.list_removable_drives()
            # display string for the dropdown
            combo_items = [drive_display_name(d) for d in current_drives]

            if combo_items != dpg.get_item_configuration('drive_combo')['items']:
                dpg.configure_item('drive_combo', items=combo_items)
                if combo_items and not dpg.get_value('drive_combo'):
                    dpg.set_value('drive_combo', combo_items[0])
                elif not combo_items:
                    dpg.set_value('drive_combo', '')
            
            state['drives'] = current_drives
        except Exception:
            pass

        time.sleep(2) 


## -- Callbacks 

def file_selected_cb(sender, app_data):

    state['iso_path'] = app_data['file_path_name']
    dpg.set_value('iso_text', state['iso_path'])
    dpg.set_value('status_text', 'Ready')


def cancel_cb():

    if state['is_burning'] and state['abort_token']:
        dpg.set_value('status_text', 'Cancelling...')
        state['abort_token'].abort()


def cleanup_ui():

    dpg.configure_item('btn_start', show=True)
    dpg.configure_item('btn_cancel', show=False)
    state['is_burning'] = False
    state['abort_token'] = None


def start_burn_cb():
    
    if not state['iso_path'] or not dpg.get_value('drive_combo'):
        return
    state['is_burning'] = True
    threading.Thread(target=burn_worker, daemon=True).start()


def burn_worker():

    dpg.configure_item('btn_start', show=False)
    dpg.configure_item('btn_cancel', show=True)
    dpg.set_value('progress_bar', 0.0)
    dpg.set_value('log_console', '--- STARTING BURN PROCESS ---\n')
    
    # Extract the raw device path from the dropdown string (e.g., '/dev/sda')
    selected_str = dpg.get_value('drive_combo')
    if not selected_str:
        dpg.set_value('status_text', 'Error: No drive selected.')
        cleanup_ui()
        return
        
    device_path = selected_str.split('(')[-1].strip(')')
    
    try:
        # Inspect the ISO
        dpg.set_value('status_text', 'Inspecting ISO...')
        stats = libiso.inspect_image(state['iso_path'])
        has_large_file = getattr(stats, 'has_large_file', False)
        supports_uefi = getattr(getattr(stats, 'boot_info', None), 'supports_uefi', False)
        partition_scheme = 'GPT' if supports_uefi else 'MBR'
        
        # Get UEFI bridge if needed
        uefi_path = libiso.ensure_uefi_bridge() if has_large_file else None
        
        # Generate safe USB label
        iso_label = libiso.extract_iso_label(state['iso_path'])
        short_label = iso_label[:11].replace(' ', '_').upper()
        
        # Create AbortToken
        state['abort_token'] = libiso.AbortToken()
        
        # Call the Rust Backend
        stream = libiso.write_image_iso(
            image_path=state['iso_path'],
            device_path=device_path,
            has_large_file=has_large_file,
            usb_label=short_label,
            partition_scheme=partition_scheme,
            uefi_ntfs_path=uefi_path,
            persistence_size_mb=None,
            verify_written=dpg.get_value('chk_verify'),
            abort_token=state['abort_token']
        )
        
        last_ui_update = 0

        for event in stream:
            if event.msg_type == 'PROGRESS':
                
                if (current_time := time.time()) - last_ui_update > 0.05 or event.written == event.total:
                    progress = event.written / event.total if event.total > 0 else 0.0
                    dpg.set_value("progress_bar", progress)
                    last_ui_update = current_time
            
            elif event.msg_type == 'PHASE':
                dpg.set_value('status_text', f'Status: {event.text}')
                log_current = dpg.get_value('log_console')
                dpg.set_value('log_console', log_current + f'\n[*] {event.text}...')
                # Auto-scroll log to bottom
                dpg.set_y_scroll('log_window', dpg.get_y_scroll_max('log_window'))
                
            elif event.msg_type == 'LOG':
                log_current = dpg.get_value('log_console')
                dpg.set_value('log_console', log_current + f'\n    {event.text}')
                
            elif event.msg_type == 'DONE':
                dpg.set_value('status_text', 'Success!')
                dpg.set_value('progress_bar', 1.0)
                
                # Fetch and print the ASCII map
                layout_map = libiso.generate_disk_layout_ascii(device_path)
                log_current = dpg.get_value('log_console')
                dpg.set_value('log_console', log_current + '\n\n' + layout_map)
                break
                
            elif event.msg_type == 'ERROR':
                dpg.set_value('status_text', 'FAILED')
                log_current = dpg.get_value('log_console')
                dpg.set_value('log_console', log_current + f'\n[!] ERROR: {event.text}')
                dpg.configure_item('progress_bar', color=(200, 50, 50, 255)) # Turn bar red
                break

    except Exception as e:
        dpg.set_value('status_text', 'Exception caught')
        log_current = dpg.get_value('log_console')
        dpg.set_value('log_console', log_current + f'\n[!] EXCEPTION: {str(e)}')
        
    cleanup_ui()


# Dynamic Zooming 

scale_factor = get_dpi_scale()
font_size = 20 * scale_factor

current_font_size = int(20 * get_dpi_scale())
current_title_size = int(32 * scale_factor) # Our new bigger font size
main_font = None # We will store the font ID here so the callback can update it
title_font = None

def zoom_font_cb(sender, app_data):

    global current_font_size, main_font, current_title_size, title_font
    
    if dpg.is_key_down(dpg.mvKey_ModCtrl):
        if main_font is not None:
            # app_data contains the scroll delta (+1.0 for up, -1.0 for down)
            # scaling by 2 pixels per scroll tick to make it feel responsive
            delta = int(app_data * 2) 
            new_size = current_font_size + delta
            
            if 10 <= new_size <= 120:
                current_font_size = new_size
                # DPG v2.3 - live re-render of the font atlas
                dpg.configure_item(main_font, size=current_font_size)

                current_title_size = int(new_size * 1.6)
                dpg.configure_item(title_font, size=current_title_size)


## -- DPG Layout 


dpg.create_context()

# -   Fonts 
# must be loaded before the UI is built 
if os.name == 'nt':
    font_path = 'C:/Windows/Fonts/segoeui.ttf'
else:
    # Common Linux fallback fonts
    linux_fonts = [
        '/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
        '/usr/share/fonts/ubuntu/Ubuntu-R.ttf',
        '/usr/share/fonts/noto/NotoSans-Regular.ttf'
    ]
    font_path = next((f for f in linux_fonts if os.path.exists(f)), '')

current_dir = __file__.replace('\\', '/').rsplit('/', 1)[0] + os.sep
font_path = current_dir + 'HackNerdFontPropo-Regular.ttf'

with dpg.font_registry():
    if os.path.exists(font_path):
        main_font = dpg.add_font(font_path, current_font_size)
        title_font = dpg.add_font(font_path, current_title_size)
        dpg.bind_font(main_font)
    else:
        print('Warning: Could not find a TTF system font. Falling back to default.')

# -  Themes 

# listen for a Left-Click to open the hidden file dialog
with dpg.item_handler_registry(tag="iso_click_handler"):
    dpg.add_item_clicked_handler(button=dpg.mvMouseButton_Left, callback=lambda: dpg.show_item('file_dialog'))

# Blue theme for buttons and headers
with dpg.theme(tag='blue_ui_theme'):
    with dpg.theme_component(dpg.mvAll): 
        dpg.add_theme_color(dpg.mvThemeCol_Button, (41, 128, 185, 255))
        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (52, 152, 219, 255))
        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive, (31, 97, 141, 255))
        
        dpg.add_theme_color(dpg.mvThemeCol_Header, (41, 128, 185, 255))
        dpg.add_theme_color(dpg.mvThemeCol_HeaderHovered, (52, 152, 219, 255))
        dpg.add_theme_color(dpg.mvThemeCol_HeaderActive, (31, 97, 141, 255))

# Global Theme
with dpg.theme() as global_theme:
    with dpg.theme_component(dpg.mvAll):
        dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 4)
        dpg.add_theme_style(dpg.mvStyleVar_WindowRounding, 6)

    with dpg.theme_component(dpg.mvButton):
        # 0 extra horizontal padding, and 10 pixels of vertical padding for buttons
        dpg.add_theme_style(dpg.mvStyleVar_FramePadding, 0, 10)

dpg.bind_theme(global_theme)


#   -  Build the UI 
with dpg.window(tag='main_window', label='libiso', no_collapse=True, no_close=True):
    
    # dpg.add_text('libiso USB Flasher', tag='main_headline')
    # if title_font: # Only bind if the font successfully loaded
    #     dpg.bind_item_font('main_headline', title_font)
        
    # dpg.add_separator()
    dpg.add_spacer(height=10)
    
    # Admin Warning
    if not state['admin']:
        dpg.add_text('libiso needs to run as Administrator/Root to access to USB devices', color=(255, 100, 100))
        dpg.add_separator()

    dpg.add_text('Drive Properties')
    dpg.add_combo(tag='drive_combo', items=[], width=-1)
    
    # ISO Selection

    dpg.add_spacer(height=50)
    dpg.add_text('ISO Selection')
    
    dpg.add_input_text(tag='iso_text', readonly=True, default_value='Click to select ISO...', width=-1)
    dpg.bind_item_handler_registry('iso_text', 'iso_click_handler')
    with dpg.tooltip('iso_text'):
        dpg.add_text("Click to browse your computer for an ISO file")

    dpg.add_spacer(height=50)
    dpg.add_separator()
                
    
    # Advanced Options
    with dpg.collapsing_header(tag='advanced_header', label='Advanced Options'):
        dpg.add_checkbox(tag='chk_verify', label='Verify written data (Bit-for-bit check)')
        dpg.add_checkbox(tag='chk_dd', label='Force DD (Raw Image) Mode')
    with dpg.tooltip('chk_verify'):
        dpg.add_text("ISO mode is the default, since it doesn't take up the entir drive space")

    # dpg.bind_item_theme('advanced_header', 'blue_ui_theme')
    
    dpg.add_spacer(height=50)
    
    # Status & Progress
    dpg.add_text('Ready', tag='status_text')
    dpg.add_progress_bar(tag='progress_bar', default_value=0.0, width=-1, height=20)
    
    dpg.add_spacer(height=50)
    
    # Buttons
    with dpg.table(header_row=False, borders_innerH=False, borders_innerV=False, borders_outerH=False, borders_outerV=False):
        # Left spacer column - stretches to push the buttons right
        # Right spacer column - stretches to push the buttons left
        dpg.add_table_column(width_stretch=True, init_width_or_weight=1.0)
        dpg.add_table_column(width_fixed=True, init_width_or_weight=200)
        dpg.add_table_column(width_stretch=True, init_width_or_weight=1.0)

        with dpg.table_row():
            dpg.add_spacer() # Empty left cell
            
            # Put BOTH buttons in the center cell group
            # When one hides and the other shows, they will both be centered
            with dpg.group():
                dpg.add_button(tag='btn_start', label='START', width=200, callback=start_burn_cb, enabled=state['admin'])
                dpg.add_button(tag='btn_cancel', label='CANCEL', width=200, callback=cancel_cb, show=False)
                
            dpg.add_spacer() # Empty right cell

    dpg.add_spacer(height=50)
    
    # Live Log Window
    # dpg.add_text('Debug Log:')
    with dpg.child_window(tag='log_window', width=-1, height=-1):
        dpg.add_text('', tag='log_console', wrap=0)

# Hidden File Dialog
dialog_width = int(700 * scale_factor)
dialog_height = int(400 * scale_factor)

with dpg.file_dialog(directory_selector=False, show=False, tag='file_dialog', callback=file_selected_cb, width=dialog_width, height=dialog_height):
    dpg.add_file_extension('ISO Files (*.iso){.iso}', color=(0, 255, 0, 255))
    dpg.add_file_extension('.*')

# Start background poller
threading.Thread(target=drive_poller, daemon=True).start()

# Register the global mouse wheel handler
with dpg.handler_registry():
    dpg.add_mouse_wheel_handler(callback=zoom_font_cb)

# Scale the main window size 
window_width = int(750 * scale_factor)
window_height = int(500 * scale_factor)

dpg.create_viewport(title='libiso USB flasher', width=window_width, height=window_height, resizable=True)

icon_path = current_dir + 'libiso.png'
if not os.path.exists(icon_path):
    import tempfile
    TRANSPARENT_PNG_B64 = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x04\x00\x00\x00\xb5\x1c\x0c\x02\x00\x00\x00\x0bIDATx\xdacd`\x00\x00\x00\x06\x00\x020\x81\xd0/\x00\x00\x00\x00IEND\xaeB`\x82'
    fd, icon_path = tempfile.mkstemp(suffix='.png')
    with os.fdopen(fd, 'wb') as f:
        f.write(TRANSPARENT_PNG_B64)
    
dpg.set_viewport_small_icon(icon_path)
dpg.set_viewport_large_icon(icon_path)

dpg.setup_dearpygui()
# dpg.show_font_manager()
dpg.set_primary_window('main_window', True)
dpg.show_viewport()
dpg.start_dearpygui()
dpg.destroy_context()