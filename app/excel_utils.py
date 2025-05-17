async def resize_image(image_path: str, logger: Optional[logging.Logger] = None) -> bool:
    logger = logger or default_logger
    try:
        logger.debug(f"📂 Attempting to open image: {image_path}")
        async with aiofiles.open(image_path, 'rb') as f:
            img_data = await f.read()
        img = await asyncio.to_thread(IMG2.open, BytesIO(img_data))
        
        # Check if the image failed to open
        if img is None:
            logger.error(f"❌ Failed to open image: {image_path}")
            return False
        
        MAXSIZE = 130

        # Convert image modes if necessary
        if img.mode == 'RGBA':
            logger.info(f"🌈 Converting RGBA image to RGB with white background: {image_path}")
            background = await asyncio.to_thread(IMG2.new, 'RGB', img.size, (255, 255, 255))
            background = await asyncio.to_thread(background.paste, img, mask=img.split()[3])
            img = background
        elif img.mode != 'RGB':
            logger.info(f"🌈 Converting {img.mode} image to RGB: {image_path}")
            img = await asyncio.to_thread(img.convert, 'RGB')

        # Background color detection and replacement functions
        def get_background_color(img):
            width, height = img.size
            pixels = np.array(img)
            top = pixels[0, :]
            bottom = pixels[height-1, :]
            left = pixels[1:height-1, 0]
            right = pixels[1:height-1, width-1]
            border_pixels = np.concatenate((top, bottom, left, right))
            color_counts = Counter(map(tuple, border_pixels))
            most_common_color, count = color_counts.most_common(1)[0]
            total_border_pixels = border_pixels.shape[0]
            if count / total_border_pixels >= 0.9:
                return most_common_color
            return None

        def is_white(color, threshold=240):
            r, g, b = color
            return r >= threshold and g >= threshold and b >= threshold

        def replace_background(img, bg_color):
            pixels = np.array(img)
            bg_color = np.array(bg_color)
            white = np.array([255, 255, 255])
            diff = np.abs(pixels - bg_color)
            mask = np.all(diff <= 5, axis=2)
            pixels[mask] = white
            return IMG2.fromarray(pixels)

        # Replace background color if needed
        background_color = await asyncio.to_thread(get_background_color, img)
        if background_color and not await asyncio.to_thread(is_white, background_color):
            logger.info(f"🖌️ Replacing background color {background_color} with white for {image_path}")
            img = await asyncio.to_thread(replace_background, img, background_color)

        # Resize image if necessary
        h, w = img.height, img.width
        logger.debug(f"📐 Original size: height={h}, width={w}")
        if h > MAXSIZE or w > MAXSIZE:
            if h > w:
                w = int(w * MAXSIZE / h)
                h = MAXSIZE
            else:
                h = int(h * MAXSIZE / w)
                w = MAXSIZE
            logger.debug(f"🔍 Resizing to: height={h}, width={w}")
            new_img = await asyncio.to_thread(img.resize, (w, h))
        else:
            new_img = img

        # Save the processed image
        buffer = BytesIO()
        await asyncio.to_thread(new_img.save, buffer, format='PNG')
        async with aiofiles.open(image_path, 'wb') as f:
            await f.write(buffer.getvalue())
        logger.info(f"✅ Image processed and saved: {image_path}")

        # Verify the file was saved
        if await aiofiles.os.path.exists(image_path):
            logger.debug(f"📏 File size after save: {(await aiofiles.os.stat(image_path)).st_size} bytes")
        else:
            logger.error(f"❌ File not found after save: {image_path}")
            return False
        return True
    except Exception as e:
        logger.error(f"❌ Error resizing image: {e}, for image: {image_path}", exc_info=True)
        return False