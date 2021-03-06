package cn.itcast.tag.web.basictag.service.impl;

import cn.itcast.tag.web.basictag.bean.BasicTagBean;
import cn.itcast.tag.web.basictag.bean.form.BasicModelRuleFormBean;
import cn.itcast.tag.web.basictag.bean.form.BasicTagFormBean;
import cn.itcast.tag.web.basictag.bean.form.UserTagFormBean;
import cn.itcast.tag.web.basictag.mapper.BasicModelRuleMapper;
import cn.itcast.tag.web.basictag.mapper.BasicTagMapper;
import cn.itcast.tag.web.basictag.mapper.UserTagMapMapper;
import cn.itcast.tag.web.basictag.service.BasicTagService;
import cn.itcast.tag.web.commons.parser.MetaParser;
import cn.itcast.tag.web.engine.bean.EngineBean;
import cn.itcast.tag.web.engine.bean.MetaDataBean;
import cn.itcast.tag.web.engine.bean.ModelBean;
import cn.itcast.tag.web.engine.bean.RuleBean;
import cn.itcast.tag.web.engine.mapper.MetaDataMapper;
import cn.itcast.tag.web.engine.mapper.ModelMapper;
import cn.itcast.tag.web.engine.mapper.RuleMapper;
import cn.itcast.tag.web.engine.service.EngineService;
import cn.itcast.tag.web.etc.listener.TagTaskPublisher;
import cn.itcast.tag.web.search.service.SearchService;
import cn.itcast.tag.web.user.bean.RoleBean;
import cn.itcast.tag.web.user.bean.UserBean;
import cn.itcast.tag.web.user.bean.UserRoleMapBean;
import cn.itcast.tag.web.user.mapper.DataMapper;
import cn.itcast.tag.web.user.service.MyShiro;
import cn.itcast.tag.web.utils.HdfsUtil;
import org.apache.shiro.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.File;
import java.util.*;
import java.util.Map.Entry;

/**
 * BasicTagService
 *
 * @author zhangwenguo
 */
@Service
@Transactional
public class BasicTagServiceImpl implements BasicTagService {

    private final static String permIds = "view;del,add,edit";
    // ????????????
    private final static int LEVELONE = 1;
    private final static int LEVELTWO = 2;
    private final static int LEVELTHREE = 3;
    private final static int LEVELFOUR = 4;
    // ???????????????
    private final static int ZERO = 0;
    private final static int ONE = 1;
    private final static int TWO = 2;
    private final static int THREE = 3;
    private final static int MINUSONE = -1;
    private final static int MINUSELEVEN = -11;
    private final static int MINUSTWELVE = -12;
    private final static int MINUSTHIRTEEN = -13;
    private Logger logger = LoggerFactory.getLogger(getClass());
    @Resource
    private BasicTagMapper basicTagMapper;
    @Resource
    private UserTagMapMapper userTagMapMapper;
    @Resource
    private ModelMapper modelMapper;
    @Resource
    private DataMapper dataMapper;
    @Resource
    private BasicModelRuleMapper basicModelRuleMapper;
    @Resource
    private RuleMapper ruleMapper;
    @Resource
    private MetaDataMapper metaDataMapper;
    @Resource
    private TagTaskPublisher tagTaskPublisher;
    @Resource
    private EngineService engineService;
    @Resource
    private SearchService searchService;
    @Value("${engineType}")
    private String engineType;
    @Value("${model.path}")
    private String modelPath;

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public int addMainBasicTag(List<BasicTagBean> beans, UserBean loginUser, RoleBean roleBean) {
        // ????????????1??????????????????
        // ????????????0????????????
        // ??????-11????????????????????????
        // ??????-12????????????????????????
        // ??????-13????????????????????????
        BasicTagBean basicTagBeanOne = null;
        BasicTagBean basicTagBeanTwo = null;
        BasicTagBean basicTagBeanThree = null;
        UserTagFormBean userTagFormBean = null;
        boolean flagOne = false;
        boolean flagTwo = false;
        boolean flagThree = false;
        // ?????????????????????????????????????????????????????????
        int state = ONE;
        if (isAdministrator()) {
            state = THREE;
        }
        try {
            for (BasicTagBean val : beans) {
                // ???????????????
                if (val.getLevel() == LEVELONE && !"".equals(val.getName())) {
                    basicTagBeanOne = new BasicTagBean();
                    basicTagBeanOne.setState(state);
                    basicTagBeanOne.setLevel(val.getLevel());
                    basicTagBeanOne.setName(val.getName());
                } else if (val.getLevel() == LEVELTWO && !"".equals(val.getName())) {
                    basicTagBeanTwo = new BasicTagBean();
                    basicTagBeanTwo.setState(state);
                    basicTagBeanTwo.setLevel(val.getLevel());
                    basicTagBeanTwo.setName(val.getName());
                } else if (val.getLevel() == LEVELTHREE && !"".equals(val.getName())) {
                    basicTagBeanThree = new BasicTagBean();
                    basicTagBeanThree.setState(state);
                    basicTagBeanThree.setLevel(val.getLevel());
                    basicTagBeanThree.setName(val.getName());
                }
            }

            // flag???false??????????????????true???????????????
            // ?????????????????????????????????
            if (basicTagBeanOne != null && basicTagBeanTwo == null && basicTagBeanThree == null) {
                basicTagBeanOne.setPid(-1);
                flagOne = basicTagMapper.isExistBasicTagForName(basicTagBeanOne, loginUser, roleBean).isEmpty();
                if (!flagOne) { // ??????????????????
                    return MINUSELEVEN;
                }
                // ????????????
                basicTagMapper.addMainBasicTag(basicTagBeanOne);
                if (basicTagBeanOne.getId() > 0) {
                    userTagFormBean = new UserTagFormBean();
                    userTagFormBean.setUserId(loginUser.getId());
                    userTagFormBean.setTagId(basicTagBeanOne.getId());
                    userTagFormBean.setPerm_ids(permIds);
                    userTagFormBean.setState(basicTagBeanOne.getState());
                    basicTagMapper.insertUserTagMap(userTagFormBean);
                } else {
                    return ZERO;
                }
            }

            // ?????????????????????????????????
            if (basicTagBeanOne != null && basicTagBeanTwo != null && basicTagBeanThree == null) {
                basicTagBeanOne.setPid(-1);
                List<BasicTagBean> beanOne = basicTagMapper.isExistBasicTagForName(basicTagBeanOne, loginUser,
                        roleBean);
                flagOne = beanOne.isEmpty();
                if (flagOne) { // ?????????????????????
                    // ????????????
                    basicTagMapper.addMainBasicTag(basicTagBeanOne);
                    if (basicTagBeanOne.getId() > 0) {
                        userTagFormBean = new UserTagFormBean();
                        userTagFormBean.setUserId(loginUser.getId());
                        userTagFormBean.setTagId(basicTagBeanOne.getId());
                        userTagFormBean.setPerm_ids(permIds);
                        userTagFormBean.setState(basicTagBeanOne.getState());
                        basicTagMapper.insertUserTagMap(userTagFormBean);
                    } else {
                        return ZERO;
                    }
                }
                // ???????????????ID??????????????????PID
                if (basicTagBeanOne.getId() > 0) {
                    basicTagBeanTwo.setPid(basicTagBeanOne.getId());
                } else {
                    if (beanOne.size() == 1) {
                        basicTagBeanTwo.setPid(beanOne.get(0).getId());
                    } else {
                        return ZERO;
                    }
                }
                List<BasicTagBean> beanTwo = basicTagMapper.isExistBasicTagForName(basicTagBeanTwo, loginUser,
                        roleBean);
                flagTwo = beanTwo.isEmpty();
                if (!flagTwo) { // ?????????????????????????????????????????????
                    return MINUSTWELVE;
                }
                // ????????????
                basicTagMapper.addMainBasicTag(basicTagBeanTwo);
                if (basicTagBeanTwo.getId() > 0) {
                    userTagFormBean = new UserTagFormBean();
                    userTagFormBean.setUserId(loginUser.getId());
                    userTagFormBean.setTagId(basicTagBeanTwo.getId());
                    userTagFormBean.setPerm_ids(permIds);
                    userTagFormBean.setState(basicTagBeanTwo.getState());
                    basicTagMapper.insertUserTagMap(userTagFormBean);
                }
            }
            // ????????????????????????
            if (basicTagBeanOne != null && basicTagBeanTwo != null && basicTagBeanThree != null) {
                basicTagBeanOne.setPid(-1);
                List<BasicTagBean> beanOne = basicTagMapper.isExistBasicTagForName(basicTagBeanOne, loginUser,
                        roleBean);
                flagOne = beanOne.isEmpty();
                if (flagOne) { // ?????????????????????
                    // ????????????
                    basicTagMapper.addMainBasicTag(basicTagBeanOne);
                    if (basicTagBeanOne.getId() > 0) {
                        userTagFormBean = new UserTagFormBean();
                        userTagFormBean.setUserId(loginUser.getId());
                        userTagFormBean.setTagId(basicTagBeanOne.getId());
                        userTagFormBean.setPerm_ids(permIds);
                        userTagFormBean.setState(basicTagBeanOne.getState());
                        basicTagMapper.insertUserTagMap(userTagFormBean);
                    } else {
                        return ZERO;
                    }
                }
                // ???????????????ID??????????????????PID
                if (basicTagBeanOne.getId() > 0) {
                    basicTagBeanTwo.setPid(basicTagBeanOne.getId());
                } else {
                    if (beanOne.size() == 1) {
                        basicTagBeanTwo.setPid(beanOne.get(0).getId());
                    } else {
                        return ZERO;
                    }
                }
                List<BasicTagBean> beanTwo = basicTagMapper.isExistBasicTagForName(basicTagBeanTwo, loginUser,
                        roleBean);
                flagTwo = beanTwo.isEmpty();
                if (flagTwo) { // ?????????????????????
                    // ????????????
                    basicTagMapper.addMainBasicTag(basicTagBeanTwo);
                    if (basicTagBeanTwo.getId() > 0) {
                        userTagFormBean = new UserTagFormBean();
                        userTagFormBean.setUserId(loginUser.getId());
                        userTagFormBean.setTagId(basicTagBeanTwo.getId());
                        userTagFormBean.setPerm_ids(permIds);
                        userTagFormBean.setState(basicTagBeanTwo.getState());
                        basicTagMapper.insertUserTagMap(userTagFormBean);
                    }
                }
                // ???????????????ID??????????????????PID
                if (basicTagBeanTwo.getId() > 0) {
                    basicTagBeanThree.setPid(basicTagBeanTwo.getId());
                } else {
                    if (beanTwo.size() == 1) {
                        basicTagBeanThree.setPid(beanTwo.get(0).getId());
                    } else {
                        return ZERO;
                    }
                }
                List<BasicTagBean> beanTree = basicTagMapper.isExistBasicTagForName(basicTagBeanThree, loginUser,
                        roleBean);
                flagThree = beanTree.isEmpty();
                if (!flagThree) { // ?????????????????????????????????
                    return MINUSTHIRTEEN;
                }
                // ????????????
                basicTagMapper.addMainBasicTag(basicTagBeanThree);
                if (basicTagBeanThree.getId() > 0) {
                    userTagFormBean = new UserTagFormBean();
                    userTagFormBean.setUserId(loginUser.getId());
                    userTagFormBean.setTagId(basicTagBeanThree.getId());
                    userTagFormBean.setPerm_ids(permIds);
                    userTagFormBean.setState(basicTagBeanThree.getState());
                    basicTagMapper.insertUserTagMap(userTagFormBean);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== addBasicTag@err:{} ====", e);
            throw e;
        }
        return ONE;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public BasicTagBean addFifthBasicTag(BasicTagBean basicTagBean, UserBean loginUser) {
        try {
            int state = 1;
            if (isAdministrator()) {
                state = 3;
            }
            basicTagBean.setState(state);
            if (basicTagBean.getLevel() == 5) {
                basicTagBean.setIndustry("??????");
            }
            basicTagMapper.addFifthBasicTag(basicTagBean);
            if (basicTagBean.getId() > 0) {
                UserTagFormBean userTagFormBean = new UserTagFormBean();
                userTagFormBean.setUserId(loginUser.getId());
                userTagFormBean.setTagId(basicTagBean.getId());
                userTagFormBean.setPerm_ids(permIds);
                userTagFormBean.setState(basicTagBean.getState());
                basicTagMapper.insertUserTagMap(userTagFormBean);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== addFifthBasicTag@err:{} ====", e);
        }
        return basicTagBean;
    }

    /**
     * ?????????????????????????????????
     *
     * @param allTags
     * @return
     */
    @Override
    public List<BasicTagBean> getBasicTagTree(List<BasicTagBean> allTags) {
        if (null == allTags || allTags.size() == 0) {
            return null;
        }
        Map<Integer, List<BasicTagBean>> levels = new TreeMap<>();
        for (BasicTagBean bean : allTags) {
            logger.info("==== {},{} ====", bean.getId(), bean.getLevel());
            if (!levels.containsKey(bean.getLevel())) {
                List<BasicTagBean> item = new ArrayList<BasicTagBean>();
                item.add(bean);
                levels.put(bean.getLevel(), item);
            } else {
                levels.get(bean.getLevel()).add(bean);
            }
        }
        if (null == levels || levels.size() == 0) {
            return null;
        }
        LinkedList<List<BasicTagBean>> orderTags = new LinkedList<List<BasicTagBean>>();
        for (Entry<Integer, List<BasicTagBean>> entry : levels.entrySet()) {
            orderTags.add(entry.getValue());
        }
        if (null == orderTags || orderTags.size() == 0) {
            return null;
        }
        for (int i = levels.size() - 1; i >= 0; i--) {
            List<BasicTagBean> curList = orderTags.get(i);
            if (i > 0) {
                List<BasicTagBean> parentList = orderTags.get(i - 1);
                for (BasicTagBean curBean : curList) {
                    long pid = curBean.getPid();
                    for (BasicTagBean parBean : parentList) {
                        if (parBean.getId() == pid) {
                            List<BasicTagBean> subTags = parBean.getSubTags();
                            if (null == subTags) {
                                parBean.setSubTags(new ArrayList<>());
                            }
                            parBean.getSubTags().add(curBean);
                        }
                    }
                }
            }
        }
        return orderTags.get(0);
    }

    /**
     * ??????????????????????????????????????????
     *
     * @param allTags
     * @return
     */
    @Override
    public List<BasicTagFormBean> getIdentifyBasicTagTree(List<BasicTagFormBean> allTags) {
        if (null == allTags || allTags.size() == 0) {
            return null;
        }
        Map<Integer, List<BasicTagFormBean>> levels = new TreeMap<>();
        for (BasicTagFormBean bean : allTags) {
            logger.info("==== {},{} ====", bean.getId(), bean.getLevel());
            if (!levels.containsKey(bean.getLevel())) {
                List<BasicTagFormBean> item = new ArrayList<BasicTagFormBean>();
                item.add(bean);
                levels.put(bean.getLevel(), item);
            } else {
                levels.get(bean.getLevel()).add(bean);
            }
        }
        if (null == levels || levels.size() == 0) {
            return null;
        }
        LinkedList<List<BasicTagFormBean>> orderTags = new LinkedList<List<BasicTagFormBean>>();
        for (Entry<Integer, List<BasicTagFormBean>> entry : levels.entrySet()) {
            orderTags.add(entry.getValue());
        }
        if (null == orderTags || orderTags.size() == 0) {
            return null;
        }
        for (int i = levels.size() - 1; i >= 0; i--) {
            List<BasicTagFormBean> curList = orderTags.get(i);
            if (i > 0) {
                List<BasicTagFormBean> parentList = orderTags.get(i - 1);
                for (BasicTagFormBean curBean : curList) {
                    long pid = curBean.getPid();
                    for (BasicTagFormBean parBean : parentList) {
                        if (parBean.getId() == pid) {
                            List<BasicTagFormBean> subTags = parBean.getSubTags();
                            if (null == subTags) {
                                parBean.setSubTags(new ArrayList<>());
                            }
                            parBean.getSubTags().add(curBean);
                        }
                    }
                }
            }
        }
        return orderTags.get(0);
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    public List<BasicTagBean> queryForAuthTag(UserBean user, RoleBean rule) {
        List<BasicTagBean> allTags = new ArrayList<BasicTagBean>();
        Long userId = user.getId();
        Long roleId = rule.getId();
        if (roleId == 1) {// ???????????????
            List<BasicTagBean> tags = dataMapper.getBasicTag();
            allTags.addAll(tags);
        }
        if (roleId == 2 || roleId == 3) {// 2??????????????????3????????????
            List<BasicTagBean> roleTags = dataMapper.getRoleBasicTag(roleId);
            List<BasicTagBean> userTags = dataMapper.getUserBasicTag(userId);
            allTags.addAll(roleTags);
            allTags.addAll(userTags);
        }
        return allTags;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryBasicTagForLevel(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean = null;
        try {
            listBean = basicTagMapper.queryBasicTagForLevel(bean, loginUser, roleBean);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForLevel@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryBasicTagForId(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean = null;
        try {
            listBean = basicTagMapper.queryBasicTagForId(bean, loginUser, roleBean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForId@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicModelRuleFormBean> queryFourthBasicTagForId(BasicModelRuleFormBean bean, UserBean loginUser) {
        List<BasicModelRuleFormBean> listBean = null;
        try {
            listBean = basicModelRuleMapper.queryFourthBasicTagForId(bean, loginUser);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryFourthBasicTagForId@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public int delBasicTagForId(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean = null;
        int state = 0;
        try {
            listBean = basicTagMapper.queryBasicTagForUserId(bean, loginUser, roleBean);
            if (listBean.size() > 0) {
                // ??????????????????????????????????????????
                return TWO;
            } else {
                if (bean.getLevel() == LEVELFOUR) {
                    // ?????????????????????????????????????????????
                    EngineBean engineBean = new EngineBean();
                    engineBean.setTagId(bean.getId());
                    boolean stop = false;
                    if (engineType.equals("1")) {
                        stop = engineService.stopEngine(engineBean, loginUser);
                        stop = true;
                    } else {
                        // ??????RAM ???????????????????????????????????????????????????job??????
                        engineService.stopEngineByQuartz(engineBean, loginUser);
                        stop = true;
                    }
                    if (!stop) {
                        return ZERO;
                    }
                    long tagId = bean.getId();
                    ModelBean modelBean = new ModelBean();
                    modelBean.setTagId(tagId);
                    if (modelMapper.delModelForTagId(modelBean) > 0) {
                        UserTagFormBean userTagFormBean = new UserTagFormBean();
                        userTagFormBean.setTagId(tagId);
                        state = basicTagMapper.delUserTagForId(userTagFormBean);
                        if (state > 0) {
                            state = basicTagMapper.delBasicTagForId(bean);
                            if (state > 0) {
                                return ONE;
                            }
                        } else {
                            return ZERO;
                        }
                    } else {
                        return ZERO;
                    }
                } else {
                    UserTagFormBean userTagFormBean = new UserTagFormBean();
                    userTagFormBean.setTagId(bean.getId());
                    state = basicTagMapper.delUserTagForId(userTagFormBean);
                    if (state > 0) {
                        state = basicTagMapper.delBasicTagForId(bean);
                        if (state > 0) {
                            return ONE;
                        } else {
                            return ZERO;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== delBasicTagForId@err:{} ====", e);
        }
        return ZERO;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public int updateMainBasicTagForId(BasicTagBean bean) {
        int state = 0;
        try {
            state = basicTagMapper.updateMainBasicTagForId(bean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== updateMainBasicTagForId@err:{} ====", e);
        }
        return state;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> searchBasicTagForName(BasicTagBean bean) {
        List<BasicTagBean> listBean = null;
        try {
            listBean = basicTagMapper.searchBasicTagForName(bean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== searchBasicTagForName@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public boolean updateFourthBasicTag(BasicModelRuleFormBean bean) {
        int state = 0;
        String hdfsFilePath = "/mengyao/tag/engine/models";
        try {
            BasicTagBean basicTagBean = new BasicTagBean();
            basicTagBean.setId(bean.getTagId());
            basicTagBean.setName(bean.getTagName());
            basicTagBean.setIndustry(bean.getIndustry());
            basicTagBean.setRule(bean.getRule());
            basicTagBean.setBusiness(bean.getBusiness());
            state = basicTagMapper.updateFourthBasicTag(basicTagBean);
            if (state > 0) {
                ModelBean modelBean = new ModelBean();
                modelBean.setTagId(bean.getTagId());
                modelBean.setModelName(bean.getModelName());
                modelBean.setModelMain(bean.getModelMain());
                modelBean.setScheTime(bean.getScheTime());
                String name = bean.getModelPath().split("\\\\")[bean.getModelPath().split("\\\\").length - 1];
                modelBean.setModelPath(hdfsFilePath + "/" + name);
                state = modelMapper.updModelForTagId(modelBean);
                if (state > 0) {
                    RuleBean ruleBean = new RuleBean();
                    ruleBean.setTagId(bean.getTagId());
                    ruleBean.setRule(bean.getRule());
                    state = ruleMapper.updRuleForTagId(ruleBean);
                    if (state > 0) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== updateFourthBasicTag@err:{} ====", e);
        }
        return false;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public int updateFifthBasicTag(BasicTagBean bean) {
        int state = 0;
        try {
            state = basicTagMapper.updateFifthBasicTag(bean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== updateFifthBasicTag@err:{} ====", e);
        }
        return state;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryMainBasicTag(BasicTagBean bean) {
        List<BasicTagBean> listBean = null;
        try {
            listBean = basicTagMapper.queryMainBasicTag(bean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryMainBasicTag@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryBasicTagForPid(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean = null;
        try {
            listBean = basicTagMapper.queryBasicTagForPid(bean, loginUser, roleBean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForPid@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryBasicTagForUserId(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean1 = null;
        try {
            listBean1 = basicTagMapper.queryBasicTagForUserId(bean, loginUser, roleBean);
            if (listBean1 != null) {
                for (BasicTagBean val1 : listBean1) {
                    List<BasicTagBean> listBean2 = basicTagMapper.queryBasicTagForUserId(val1, loginUser, roleBean);
                    val1.setSubTags(listBean2);
                    for (BasicTagBean val2 : listBean2) {
                        List<BasicTagBean> listBean3 = basicTagMapper.queryBasicTagForUserId(val2, loginUser, roleBean);
                        val2.setSubTags(listBean3);
                        for (BasicTagBean val3 : listBean3) {
                            List<BasicTagBean> listBean4 = basicTagMapper.queryBasicTagForUserId(val3, loginUser,
                                    roleBean);
                            val3.setSubTags(listBean4);
                            for (BasicTagBean val4 : listBean4) {
                                List<BasicTagBean> listBean5 = basicTagMapper.queryBasicTagForUserId(val4, loginUser,
                                        roleBean);
                                val4.setSubTags(listBean5);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForIds@err:{} ====", e);
        }

        return listBean1;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryBasicTagForWithPid(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean1 = null;
        try {
            if (bean.getLevel() == LEVELTHREE) {// ???????????????????????????
                // ????????????
                listBean1 = basicTagMapper.queryBasicTagForWithPid1(bean, loginUser, roleBean);
                if (listBean1 != null && listBean1.size() > 0) {
                    for (BasicTagBean val1 : listBean1) {
                        // ????????????
                        BasicTagBean basicTagBean = new BasicTagBean();
                        basicTagBean.setId(val1.getId());
                        List<BasicTagBean> listBean2 = basicTagMapper.queryBasicTagForWithPid1(basicTagBean, loginUser,
                                roleBean);
                        if (listBean2 != null && listBean2.size() > 0) {
                            for (BasicTagBean basicTagBean2 : listBean2) {
                                basicTagBean2.setUserCount(searchService.getUserCountByTagId(basicTagBean2.getId(), 1));
                            }
                            val1.setSubTags(listBean2);
                        }
                    }
                }
            } else if (bean.getLevel() == LEVELFOUR) {// ???????????????????????????
                listBean1 = basicTagMapper.queryBasicTagForWithPid2(bean, loginUser, roleBean);
                if (listBean1 != null && listBean1.size() > 0) {
                    for (BasicTagBean val1 : listBean1) {
                        // ??????????????????bean
                        List<BasicTagBean> listBean2 = basicTagMapper.queryBasicTagForWithPid1(bean, loginUser,
                                roleBean); // val1.getId(),curUser.getId(),bean.getName()
                        if (listBean2 != null && listBean2.size() > 0) {
                            for (BasicTagBean basicTagBean2 : listBean2) {
                                basicTagBean2.setUserCount(searchService.getUserCountByTagId(basicTagBean2.getId(), 1));
                            }
                            val1.setSubTags(listBean2);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForIds@err:{} ====", e);
        }

        return listBean1;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicModelRuleFormBean> queryBasicTagAndModelForWithPid(BasicModelRuleFormBean bean, UserBean loginUser,
                                                                        RoleBean roleBean) {
        List<BasicModelRuleFormBean> listBean1 = null;
        try {
            if (bean.getLevel() == LEVELTHREE) {// ???????????????????????????
                // ????????????
                listBean1 = basicModelRuleMapper.queryBasicTagAndModelForWithPid1(bean, loginUser, roleBean);
                if (listBean1 != null && listBean1.size() > 0) {
                    for (BasicModelRuleFormBean val1 : listBean1) {
                        // ????????????
                        BasicModelRuleFormBean basicTagBean = new BasicModelRuleFormBean();
                        basicTagBean.setTagId(val1.getTagId());
                        List<BasicModelRuleFormBean> listBean2 = basicModelRuleMapper
                                .queryBasicTagAndModelForWithPid1(basicTagBean, loginUser, roleBean);
                        if (listBean2 != null && listBean2.size() > 0) {
                            for (BasicModelRuleFormBean basicTagBean2 : listBean2) {
                                basicTagBean2
                                        .setUserCount(searchService.getUserCountByTagId(basicTagBean2.getTagId(), 1));
                            }
                            val1.setSubTags(listBean2);
                        }
                    }
                }
            } else if (bean.getLevel() == LEVELFOUR) {// ???????????????????????????
                listBean1 = basicModelRuleMapper.queryBasicTagAndModelForWithPid2(bean, loginUser, roleBean);
                if (listBean1 != null && listBean1.size() > 0) {
                    for (BasicModelRuleFormBean val1 : listBean1) {
                        // ??????????????????bean
                        List<BasicModelRuleFormBean> listBean2 = basicModelRuleMapper
                                .queryBasicTagAndModelForWithPid1(bean, loginUser, roleBean);
                        if (listBean2 != null && listBean2.size() > 0) {
                            for (BasicModelRuleFormBean basicTagBean2 : listBean2) {
                                basicTagBean2
                                        .setUserCount(searchService.getUserCountByTagId(basicTagBean2.getTagId(), 1));
                            }
                            val1.setSubTags(listBean2);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForIds@err:{} ====", e);
        }

        return listBean1;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryBasicTagForName(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> listBean = null;
        try {
            listBean = basicTagMapper.queryBasicTagForName(bean, loginUser, roleBean);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("==== queryBasicTagForPid@err:{} ====", e);
        }
        return listBean;
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    @Override
    public int addFourthBasicTag(BasicModelRuleFormBean bean, UserBean loginUser) {
        int state = 1;
        if (isAdministrator()) {
            state = 4;
        }
        bean.setState(state);
        try {
            int state1 = 0;
            int state2 = 0;
            BasicTagBean basicTagBean = new BasicTagBean();
            String tagName = bean.getTagName();
            long threeTagid = bean.getThreeTagId();
            String business = bean.getBusiness();
            String industry = bean.getIndustry();
            String rule = bean.getRule();
            int level = bean.getLevel();
            if (level == 4) {
                industry = "??????";
            }
            state = bean.getState();
            if (tagName == null || business == null || threeTagid <= 0) {
                return MINUSONE;
            }
            basicTagBean.set(tagName, threeTagid, business, industry, rule, level, state);
            RuleBean ruleBean = new RuleBean();
            ruleBean.setRule(bean.getRule());
            ModelBean modelBean = new ModelBean();
            HdfsUtil hdfsUtil = HdfsUtil.getInstance();
            if (null == bean.getModelPath()) {
                return MINUSONE;
            } else {
                // ?????????????????????
                String separatorRegex = File.separator.replace("\\","\\\\");
                String[] pathArray = bean.getModelPath().split(separatorRegex);
                String name = pathArray[pathArray.length - 1];
                // ?????????????????????
                state1 = basicTagMapper.addFourthBasicTag(basicTagBean);
                // ??????????????????
                if (state1 > 0) {
                    UserTagFormBean userTagFormBean = new UserTagFormBean();
                    userTagFormBean.setTagId(basicTagBean.getId());
                    userTagFormBean.setUserId(loginUser.getId());
                    userTagFormBean.setState(0);
                    userTagFormBean.setState(basicTagBean.getState());
                    state2 = userTagMapMapper.insertUserTagMap(userTagFormBean);
                    if (state2 > 0) {
                        // ????????????
                        modelBean.setTagId(basicTagBean.getId());
                        modelBean.setType(1);
                        //???????????????????????????hdfs?????????
                        String tagModelDir = "Tag_" + basicTagBean.getId() + "/lib/";
                        modelBean.setModelPath(modelPath + tagModelDir + name);
                        modelBean.setModelMain(bean.getModelMian());
                        modelBean.setModelName(bean.getModelName());
                        modelBean.setScheTime(bean.getScheTime());
                        modelBean.setArgs(bean.getArgs());
                        // ?????????????????????hdfs??????
                        // /app/tags/model/Tag_10
                        hdfsUtil.mkdir(modelPath + tagModelDir);
                        hdfsUtil.uploadLocalFile2HDFS(bean.getModelPath(), modelBean.getModelPath());
                        // ????????????
                        ruleBean.setTagId(basicTagBean.getId());
                        state1 = ruleMapper.addRule(ruleBean);
                        state2 = modelMapper.addModel(modelBean);
                        if (state1 > 0 && state2 > 0) {
                            MetaParser parser = MetaParser.getParser(ruleBean.getRule());
                            MetaDataBean metaBean = parser.getMeta();
                            metaBean.setTagId(basicTagBean.getId());
                            if (null != metaBean) {
                                state1 = metaDataMapper.addMetaData(metaBean);
                                if (state1 > 0) {
                                    return ONE;
                                } else {
                                    throw new Exception("???????????????????????????????????????????????????????????????");
                                }
                            }
                        } else {
                            throw new Exception("???????????????????????????????????????????????????????????????");
                        }
                    } else {
                        throw new Exception("?????????????????????????????????????????????????????????????????????");
                    }
                } else {
                    throw new Exception("???????????????????????????");
                }
            }
        } catch (Exception e) {
            logger.error("==== addFourthBasicTag@err:{} ====", e);
        }
        return ZERO;
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED, rollbackFor = Exception.class)
    @Override
    public List<BasicTagBean> queryAllBasicTags() {
        List<BasicTagBean> beans = null;
        try {
            beans = basicTagMapper.queryAllBasicTags();
            logger.info("==== queryAllBasicTags@exec:{} ====", beans);
        } catch (Exception e) {
            logger.error("==== queryAllBasicTags@err:{} ====", e);
        }
        return beans;
    }

    // ????????????
    private Boolean isAdministrator() {
        try {
            MyShiro.Principal curUser = (MyShiro.Principal) SecurityUtils.getSubject().getPrincipal();
            if (Objects.nonNull(curUser)) {
                List<UserRoleMapBean> roleMaps = curUser.getRoleMaps();
                Long roleId = roleMaps.get(0).getRoleId();
                // ???????????????
                if (roleId.equals(1L)) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("==== examine @result:??????????????? ====");
            return false;
        }
        return false;
    }

    @Override
    public int taskProcessing(BasicTagBean bean, UserBean loginUser) {
        // ???????????????1????????????2???????????????3????????????4????????????5?????????
        // ???????????????1???????????????0????????????
        int state = bean.getState();
        EngineBean engineBean = null;
        if (state == LEVELTHREE) { // ???????????????
            bean.setState(4);
            int result = basicTagMapper.updateStateForId(bean);
            if (result > 0) {
                engineBean = new EngineBean();
                engineBean.setTagId(bean.getId());
                engineBean.setRemark(bean.getRemark());
                if (engineType.equals("1")) {
                    engineService.stopEngine(engineBean, loginUser);
                } else {
                    engineService.stopEngineByQuartz(engineBean, loginUser);
                }
                return ONE;
            }
        } else if (state == LEVELFOUR) { // ???????????????
            bean.setState(3);
            int result = basicTagMapper.updateStateForId(bean);
            if (result > 0) {
                engineBean = new EngineBean();
                engineBean.setTagId(bean.getId());
                engineBean.setRemark(bean.getRemark());
                System.out.println("engineType=====" + engineType + "====" + engineType.equals("1"));
                if (engineType.equals("1")) {
                    engineService.startEngine(engineBean, loginUser);
                } else {
                    engineService.startEngineByQuartz(engineBean, loginUser);
                }
                return ONE;
            }
        } else {
            return ZERO;
        }
        return ZERO;
    }

    @Override
    public Boolean isExistForName(BasicTagBean bean, UserBean loginUser, RoleBean roleBean) {
        List<BasicTagBean> beans = Collections.emptyList();
        try {
            beans = basicTagMapper.isExistBasicTagForName(bean, loginUser, roleBean);
            logger.info("==== isExistForName@exec:{} ====", beans);
        } catch (Exception e) {
            logger.error("==== isExistForName@err:{} ====", e);
        }
        return !beans.isEmpty();
    }

    @Override
    public Long queryTagCountByLevel(int level) {
        return basicTagMapper.queryTagCountByLevel(level);
    }

}
