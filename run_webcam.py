import argparse
import logging
import time
import math
import cv2
import numpy as np

from tf_pose.estimator import TfPoseEstimator
from tf_pose.networks import get_graph_path, model_wh
# from tensorflow.python.compiler.tensorrt import trt_convert as trt
def find_point(pose, p):
    for point in pose:
        try:
            body_part = point.body_parts[p]
            return (int(body_part.x * width + 0.5), int(body_part.y * height + 0.5))
        except:
            return (0,0)
    return (0,0)
def euclidian( point1, point2):
    return math.sqrt((point1[0]-point2[0])**2 + (point1[1]-point2[1])**2 )

def angle_calc(p0, p1, p2 ):
    '''
        p1 is center point from where we measured angle between p0 and p2
    '''
    try:
        a = (p1[0]-p0[0])**2 + (p1[1]-p0[1])**2
        b = (p1[0]-p2[0])**2 + (p1[1]-p2[1])**2
        c = (p2[0]-p0[0])**2 + (p2[1]-p0[1])**2
        angle = math.acos( (a+b-c) / math.sqrt(4*a*b) ) * 180/math.pi
    except:
        return 0
    return int(angle)
def mountain_pose( a, b, c, d, e):
   
    if a in range(20,160) and b in range(60,160) and c in range(60,140) and d in range(100,145) and e in range(100,145):
        return True
    return False

def warrior_pose(a,b,z,c,d,e):

    if(a in range(172,175) and b in range(98,132) and z in range(77,174) and d>c and e>c):
        return True
    return False    

def draw_str(dst, xxx_todo_changeme, s, color, scale):
    
    (x, y) = xxx_todo_changeme
    if (color[0]+color[1]+color[2]==255*3):
        cv2.putText(dst, s, (x+1, y+1), cv2.FONT_HERSHEY_PLAIN, scale, (0, 0, 0), thickness = 4, lineType=10)
    else:
        cv2.putText(dst, s, (x+1, y+1), cv2.FONT_HERSHEY_PLAIN, scale, color, thickness = 4, lineType=10)
    #cv2.line    
    cv2.putText(dst, s, (x, y), cv2.FONT_HERSHEY_PLAIN, scale, (255, 255, 255), lineType=11)

def plank( a, b, c, d, e, f):
    #There are ranges of angle and distance to for plank. 
    '''
        a and b are angles of hands
        c and d are angle of legs
        e and f are distance between head to ankle because in plank distace will be maximum.
    '''
    if (a in range(50,100) or b in range(50,100)) and (c in range(135,175) or d in range(135,175)) and (e in range(50,250) or f in range(50,250)):
        return True
    return False


#hoefheoifwe

logger = logging.getLogger('TfPoseEstimator-WebCam')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

fps_time = 0

def str2bool(v):
    return v.lower() in ("yes", "true", "t", "1")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='tf-pose-estimation realtime webcam')
    parser.add_argument('--camera', type=str, default=0)

    parser.add_argument('--resize', type=str, default='0x0',
                        help='if provided, resize images before they are processed. default=0x0, Recommends : 432x368 or 656x368 or 1312x736 ')
    parser.add_argument('--resize-out-ratio', type=float, default=4.0,
                        help='if provided, resize heatmaps before they are post-processed. default=1.0')

    parser.add_argument('--model', type=str, default='mobilenet_thin', help='cmu / mobilenet_thin / mobilenet_v2_large / mobilenet_v2_small')
    parser.add_argument('--show-process', type=bool, default=False,
                        help='for debug purpose, if enabled, speed for inference is dropped.')
    
    parser.add_argument('--tensorrt', type=str, default="False",
                        help='for tensorrt process.')
    args = parser.parse_args()

    logger.debug('initialization %s : %s' % (args.model, get_graph_path(args.model)))
    w, h = model_wh(args.resize)
    if w > 0 and h > 0:
        e = TfPoseEstimator(get_graph_path(args.model), target_size=(w, h), trt_bool=str2bool(args.tensorrt))
    else:
        e = TfPoseEstimator(get_graph_path(args.model), target_size=(432, 368), trt_bool=str2bool(args.tensorrt))
    logger.debug('cam read+')
    cam = cv2.VideoCapture(args.camera)
    ret_val, image = cam.read()
    # logger.info('cam image=%dx%d' % (image.shape[1], image.shape[0]))

    count = 0
    i = 0
    
    frm = 0
    y1 = [0,0]
    global height,width
    orange = (0,140,255)

    while True:
        
        ret_val, image = cam.read()
        i=1
        logger.debug('image process+')
        humans = e.inference(image, resize_to_default=(w > 0 and h > 0), upsample_size=args.resize_out_ratio)

        logger.debug('postprocess+')
        image = TfPoseEstimator.draw_humans(image, humans, imgcopy=False)
        pose = humans
        height,width = image.shape[0],image.shape[1]
        if len(pose) > 0:
            head_hand_dst1 = int (euclidian(find_point(pose,0),find_point(pose,7)))
            head_hand_dst2 = int (euclidian(find_point(pose,0),find_point(pose,4)))
            m_pose = int (euclidian(find_point(pose,7),find_point(pose,4)))

            angle1= angle_calc(find_point(pose,6),find_point(pose,5),find_point(pose,1))
            angle2= angle_calc(find_point(pose,3),find_point(pose,2),find_point(pose,1))

            head_hand_dst_l = int(euclidian(find_point(pose, 0), find_point(pose, 7)))
            head_hand_dst_r = int(euclidian(find_point(pose, 0), find_point(pose, 4)))
                # angle calcucations
            angle3 =  angle_calc(find_point(pose,7), find_point(pose,6), find_point(pose,5))
            angle4 =  angle_calc(find_point(pose,11), find_point(pose,12), find_point(pose,13))
            angle6 =  angle_calc(find_point(pose,4), find_point(pose,3), find_point(pose,2))
            angle8 =  angle_calc(find_point(pose,8), find_point(pose,9), find_point(pose,10))
            angle_417= angle_calc(find_point(pose,4),find_point(pose,1),find_point(pose,7))
            angle_189= angle_calc(find_point(pose,1),find_point(pose,8),find_point(pose,9))
            angle_11112= angle_calc(find_point(pose,1),find_point(pose,11),find_point(pose,12))

            key_14= find_point(pose,14)
            key_4= find_point(pose,4)
            key_7= find_point(pose,7)
            
            mode=1

            if(mode==1):
                action= " warrior pose"
                yoga=True
                draw_str(image,(50,50),action,orange,2)
                logger.debug('warrior pose')

                
            if(mode==2):
                action= " mountain pose"
                yoga=True
                draw_str(image,(50,50),action,orange,2)
                logger.debug('mountain pose')


            if(mode==1) and warrior_pose(angle_417,angle_189,angle_11112,key_14[1],key_4[1],key_7[1]):
                # action= " warrior pose"
                # yoga=True
                # draw_str(image,(20,50),action,orange,2)
                # logger.debug('warrior pose')
                cv2.putText(image,
                    "correct",
                    (50, 100),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 255, 0), 2)
                cv2.putText(image,
                    "angle_4,1,7="+str(angle_417),
                    (50, 130),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 255, 0), 2)
                
                cv2.putText(image,
                    "angle_1,8,9=" + str(angle_189),
                    (50, 160),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 255, 0), 2)

                cv2.putText(image,
                    "angle_1,11,12=" + str(angle_11112),
                    (50, 190),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 255, 0), 2)

            elif(mode==2) and mountain_pose(m_pose,angle1,angle2,head_hand_dst1,head_hand_dst2):
                cv2.putText(image,
                    "correct",
                    (50, 100),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 255, 0), 2)

            elif (mode==3) and plank(angle3, angle6, angle4, angle8,head_hand_dst_r, head_hand_dst_l):
                 action = "Plank"
                 is_yoga = True
                            #if prev_action == 'Unknown' or prev_action == "Unknown_First":
                            #    yoga_duration = time.time()
                            #logger.debug("*** Plank ***")
                 draw_str(image, (20, 50), " Plank", orange, 2)
                 logger.debug("*** Plank ***")
            
            else:
                cv2.putText(image,
                    "wrong",
                    (50, 100),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 0, 255), 2)

                if (angle_417 not in range(172,175)):
                    cv2.putText(image,
                    "angle_4,1,7=" +str(angle_417),
                    (50, 130),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 0, 255), 2)

                if (angle_189 not in range(98,132)):
                    cv2.putText(image,
                    "angle_1,8,9="+str(angle_189),
                    (50, 160),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 0, 255), 2)

                if (angle_11112 not in range(77,174)):
                    cv2.putText(image,
                    "angle_1,11,12="+str(angle_11112),
                    (50, 190),  cv2.FONT_HERSHEY_SIMPLEX, 1,
                    (0, 0, 255), 2)
                
        
        logger.debug('show+')
        cv2.putText(image,
                    "FPS: %f" % (1.0 / (time.time() - fps_time)),
                    (10, 10),  cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                    (0, 255, 0), 2)
       
        cv2.imshow('tf-pose-estimation result', image)
        fps_time = time.time()
        if cv2.waitKey(1) == 27:
            break
        logger.debug('finished+')

    cv2.destroyAllWindows()
