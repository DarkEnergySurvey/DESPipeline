#!/usr/bin/env python

import numpy as np

DEFAULT_MINCOLS = 1    # Narrowest feature to interpolate
DEFAULT_MAXCOLS = None  # Widest feature to interpolate.  None means no limit.

def zipper_interp(image,mask,interp_mask,axis=1,**kwargs):

    """
    Calls either zipper_interp_rows (axis=1) or zipper_interp_cols (axis=2)
    """
    if axis == 1:
        return zipper_interp_rows(image,mask,interp_mask,**kwargs)
    elif axis == 2:
        return zipper_interp_cols(image,mask,interp_mask,**kwargs)
    else:
        raise ValueError("ERROR: Need to specify axis as axis=1 or axis=2")
        
def zipper_interp_rows(image,mask,interp_mask,**kwargs):

    """
    Performs zipper row interpolation.
    Based on Gary Berstein's row_interp.py inside pixcorrect

    Interpolate over selected pixels by inserting average/median of
    pixels to left and right of any bunch of adjacent selected pixels.
    If the interpolation region touches an edge, or the adjacent pixel
    has flags marking it as invalid, than the value at other border is
    used for interpolation.  No interpolation is done if both boundary
    pixels are invalid.

    :Postional parameters:
       'image': the 2D numpy array input image
       'mask':  the 2D numpy array input image
       'interp_mask': Mask bits that will trigger interpolation

    :Ouputs:
       Returns 'image' and 'mask' as tuple.
       If a BADPIX_INTERP value is passed then the modified mask with
       the interpolated pixels flaged with BADPIX_INTERP will be returned

    :Optional parameters (passed as **kwargs)
       'BADPIX_INTERP': bit value to assign to interpolated pixels (off by default)
       'min_cols': Minimum width of region to be interpolated.
       'max_cols': Maximum width of region to be interpolated.
       'invalid_mask': Mask bits invalidating a pixel as interpolation source.
       'logger' : Logger object for logging info
       'block_size' : y-size of the zipper block columns
       'ydiltate' : number of pixels to dilate in the y-axis
       'add_noise' : Add poison noise to the zipper
    """

    # Extract kwargs for optional params
    BADPIX_INTERP = kwargs.get('BADPIX_INTERP',None)
    invalid_mask  = kwargs.get('invalid_mask',0)
    min_cols    = kwargs.get('DEFAULT_MINCOLS',DEFAULT_MINCOLS)
    max_cols    = kwargs.get('DEFAULT_MAXCOLS',DEFAULT_MAXCOLS)
    logger      = kwargs.get('logger',None)
    yblock      = kwargs.get('block_size',1)
    xdilate     = kwargs.get('dilate',0)
    add_noise   = kwargs.get('add_noise',False)

    msg = 'Zipper interpolation along rows'
    if logger:logger.info(msg)
    else: print "#",msg

    # Find the pixels to work with
    interpolate = np.array(mask & interp_mask, dtype=bool)
    # Make arrays noting where a run of bad pixels starts or ends
    # Then make arrays has_?? which says whether left side is valid
    # and an array with the value just to the left/right of the run.
    work = np.array(interpolate)
    work[:,1:] = np.logical_and(interpolate[:,1:], ~interpolate[:,:-1])
    ystart,xstart = np.where(work)
    
    work = np.array(interpolate)
    work[:,:-1] = np.logical_and(interpolate[:,:-1], ~interpolate[:,1:])
    yend, xend = np.where(work)
    xend = xend + 1   # Make the value one-past-end
    
    # If we've done this correctly, every run has a start and an end.
    if not np.all(ystart==yend):
        print "Logic problem, ystart and yend not equal."
        print ystart,yend ###
        return 1
    
    # Narrow our list to runs of the desired length range
    # not touching the edges
    use = xend-xstart >= min_cols
    if max_cols is not None:
        use = np.logical_and(xend-xstart<=max_cols, use)
    use = np.logical_and(xstart>0, use)
    use = np.logical_and(xend<interpolate.shape[0], use)
    xstart = xstart[use]
    xend   = xend[use]
    ystart = ystart[use]

    # Now determine which runs have valid data at left/right
    xleft    = np.maximum(0, xstart-1)
    has_left = ~np.array(mask[ystart,xleft] & invalid_mask, dtype=bool)
    has_left = np.logical_and(xstart>=1,has_left)
    left_value = image[ystart,xleft]
    
    xright = np.minimum(work.shape[1]-1, xend)
    has_right = ~np.array(mask[ystart,xright] & invalid_mask, dtype=bool)
    has_right = np.logical_and(xend<work.shape[1],has_right)
    right_value = image[ystart,xright]

    # Define the type on edges/borders:
    # - Both left and right
    # - left only good values
    # - right only good values
    left_and_right = np.where(np.logical_and(has_left,has_right))[0]
    left_only      = np.where(np.logical_and(has_left,~has_right))[0]
    right_only     = np.where(np.logical_and(~has_left,has_right))[0]
    all_cases      = np.concatenate((left_and_right,left_only,right_only))

    # Loop over all cases (rows) to interpolate
    for run in all_cases:

        # Get the limits
        # y0 is the row we want to interpolate
        # x1,x2 are the right and left-hand edges of the region to interpolate
        # y1,y2 are the lower and upper edges of the box car block
        y0 = ystart[run]
        x1 = xstart[run] # x_left index
        x2 = xend[run]   # x_right index
        y1 = max(0,y0-yblock+1)            # lower y for block
        y2 = min(image.shape[0],y0+yblock) # upper y for block

        # Decide case and from border condition
        if run in left_only:
            im_vals = image[y1:y2,x1-1]
        elif run in list(right_only):
            im_vals = image[y1:y2,x2]
        elif run in left_and_right:
            im_vals = np.append(image[y1:y2,x1-1],image[y1:y2,x2])

        # Dilate zipper in the x-direction?
        if xdilate > 0:
            x1 = x1 - int(xdilate)
            x2 = x2 + int(xdilate)
        mu  = np.median(im_vals)
        if mu > 1 and add_noise:
            image[y0,x1:x2] = np.random.poisson(mu,x2-x1)
        else:
            image[y0,x1:x2] = mu

        if BADPIX_INTERP:
            mask[y0,x1:x2] |= BADPIX_INTERP

    return image,mask

def zipper_interp_cols(image,mask,interp_mask,**kwargs):

    """
    Performs zipper column interpolation.
    Based on Gary Berstein's coadd-prepare script.

    Interpolate over selected pixels by inserting average/median of
    pixels to top and bottom of any bunch of adjacent selected
    pixels. For column interpolation we do not attempt to determine
    invalid pixels, as it is done for row_interp. The column
    interpolation is meant for coadded images, which do not have a bit
    to flag 'invalid_mask.'

    :Postional parameters:
       'image': the 2D numpy array input image
       'mask':  the 2D numpy array input image
       'interp_mask': Mask bits that will trigger interpolation

    :Ouputs:
       Returns 'image' and 'mask' as tuple.
       If a BADPIX_INTERP value is passed then the modified mask with
       the interpolated pixels flaged with BADPIX_INTERP will be returned 

   :Optional parameters (passed as **kwargs)
       'BADPIX_INTERP': bit value to assign to interpolated pixels (off by default)
       'min_cols': Minimum width of region to be interpolated.
       'max_cols': Maximum width of region to be interpolated.
       'logger' : Logger object for logging info
       'xblock' : x-size of the zipper block columns
       'yblock' : y-size of the zipper block columns
       'ydiltate' : number of pixels to dilate in the y-axis
       'add_noise' : Add poison noise to the zipper
       'region_file': Optional output region file to store the area to be zippered
    """

    # Extract kwargs for optional params
    BADPIX_INTERP = kwargs.get('BADPIX_INTERP',None)
    min_cols    = kwargs.get('DEFAULT_MINCOLS',DEFAULT_MINCOLS)
    max_cols    = kwargs.get('DEFAULT_MAXCOLS',DEFAULT_MAXCOLS)
    logger      = kwargs.get('logger',None)
    xblock      = kwargs.get('xblock',1)
    yblock      = kwargs.get('yblock',1)
    ydilate     = kwargs.get('ydilate',0)
    add_noise   = kwargs.get('add_noise',False)
    region_file = kwargs.get('region_file',None) 

    if yblock < 0:
        yblock = 1
        msg = "yblock must be >= 0 -- forcing yblock=1" 
        if logger:logger.info(msg)
        else: print "#",msg

    if region_file:
        msg = "Will write ds9 regions to %s" % region_file
        reg = open(region_file,'w')
        if logger:logger.info(msg)
        else: print "#",msg

    # Print out options that we will use
    msg = 'Zipper interpolation along columns '
    msg = msg + "with xblock=%s, yblock=%s, maxcols=%s, mincols=%s, add_noise=%s, ydilate=%s" % (xblock,yblock, max_cols, min_cols, add_noise,ydilate)
    if logger:logger.info(msg)
    else: print "#",msg

    # Find the pixels to work with
    interpolate = np.array(mask & interp_mask, dtype=bool)
    # Identify column runs to interpolate, start by marking beginnings of runs
    work = np.array(interpolate)
    work[1:,:] = np.logical_and(interpolate[1:,:], ~interpolate[:-1,:])
    xstart,ystart = np.where(work.T)

    # Now ends of runs
    work = np.array(interpolate)
    work[:-1,:] = np.logical_and(interpolate[:-1,:], ~interpolate[1:,:])
    xend, yend = np.where(work.T)
    yend = yend + 1   # Make the value one-past-end
    
    # If we've done this correctly, every run has a start and an end, on same col
    if not np.all(xstart==xend):
        print "Logic problem, xstart and xend not equal."
        print xstart,xend ###
        return 1

    # Narrow our list to runs of the desired length
    use = yend-ystart >= min_cols
    if max_cols is not None:
        use = np.logical_and(yend-ystart<=max_cols, use)
    use = np.logical_and(ystart>0, use)
    use = np.logical_and(yend<interpolate.shape[0], use)
    ystart = ystart[use]
    yend   = yend[use]
    xstart = xstart[use]

    # Make copies of the images that we will modify/interpolate
    image_interp = np.copy(image)
    mask_interp  = np.copy(mask)
    
    # Loop over
    for run in range(len(xstart)):

        x0 = xstart[run]
        y1 = ystart[run]    # y_lower index
        y2 = yend[run] - 1  # y_upper index

        # Block x-zipper
        x1 = max(0,x0-xblock+1)
        x2 = min(image.shape[1],x0+xblock)

        # Block y-zipper limits
        if yblock > 0:
            y1a = max(0,y1-yblock)
        else:
            y1a = max(0,y1-1)
        y1b = min(image.shape[0],y1)

        y2a = max(0,y2)
        y2b = min(image.shape[0],y2+yblock)
        im_vals = np.append(image[y1a:y1b,x1:x2],image[y2a:y2b,x1:x2])

        # We want to avoid the zero values we encounter in the sapling
        idx  = np.where(im_vals != 0)
        n_good = len(idx[0])
        if n_good > 0:
            mu  = np.median(im_vals[idx])
        else: # Fall back in case they are all zero
            msg = "WARNING: All sampling values equal to zero on slices [%s:%s,%s:%s] and [%s:%s,%s:%s]" % (y1a,y1b,x1,x2, y2a,y2b,x1,x2)
            if logger:logger.info(msg)
            else: print "#",msg
            mu  = im_vals.mean()

        # y-dilate
        ya = y1 - int(ydilate)
        yb = y2 + int(ydilate)

        # Dilate the mask
        if ydilate > 0:
            mask_interp[ya:yb,x0] = interp_mask

        if mu > 1 and add_noise:
            image_interp[ya:yb,x0] = np.random.poisson(mu,yb-ya)
        else:
            image_interp[ya:yb,x0] = mu

        # Change the bit in the mask to reflect the pixel was interpolated
        if BADPIX_INTERP:
            mask_interp[ya:yb,x0] |= BADPIX_INTERP

        # ds9 style region file
        if region_file:
            reg.write("line %s %s %s %s\n" % (x0+1,y1+1,x0+1,y2+1))
            
    return image_interp,mask_interp


